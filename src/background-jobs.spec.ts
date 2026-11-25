import { runPosEvents } from './events/main';
import { runPosPayments } from './events/payment-main';
import { ModuleRef } from '@nestjs/core';
import { BackgroundJobs, BackgroundJobsHealthController } from './background-jobs';

jest.mock('./events/main', () => ({ runPosEvents: jest.fn() }));
jest.mock('./events/payment-main', () => ({ runPosPayments: jest.fn() }));
jest.mock('@nestjs/mongoose', () => ({ getConnectionToken: () => 'DatabaseConnection' }));
jest.mock('./pos/checkout.service', () => ({ CheckoutService: class CheckoutService {} }));

const runtime = [runPosEvents, runPosPayments];
const expectedModes = [['relay','worker'],['relay','worker']];
const configuration = {
  'POS_EMBED_EVENTS': 'true',
  'POS_EMBED_PAYMENTS': 'true',
  'POS_PAYMENT_RECONCILIATION_MODE': 'bullmq',
  'POS_PAYMENT_WORKER_ENABLED': 'true'
};

describe('embedded background jobs', () => {
  let jobs: BackgroundJobs;

  beforeEach(() => {
    jest.replaceProperty(process, 'env', { ...process.env });
    for (const key of [...Object.keys(configuration), 'EVENT_REDIS_URL']) delete process.env[key];
    jobs = new BackgroundJobs({ get: jest.fn() } as unknown as ModuleRef);
  });

  afterEach(async () => {
    await jobs.beforeApplicationShutdown();
    jest.restoreAllMocks();
    jest.resetAllMocks();
  });

  it('leaves standalone workers in control by default', () => {
    jobs.start();
    for (const run of runtime) expect(run).not.toHaveBeenCalled();
    expect(jobs.health()).toEqual({ status: 'ok', jobs: {} });
  });

  it('requires Redis when embedding enabled processors', () => {
    Object.assign(process.env, configuration);
    expect(() => jobs.start()).toThrow('EVENT_REDIS_URL');
  });

  it('starts each configured mode once and waits for it to stop', async () => {
    Object.assign(process.env, configuration, { EVENT_REDIS_URL: 'redis://localhost' });
    const waiters: jest.Mock[] = [];
    for (const run of runtime) {
      jest.mocked(run).mockImplementation((...args) => {
        const signal = args.at(-2) as AbortSignal;
        const ready = args.at(-1) as (ready: boolean) => void;
        return new Promise<void>(resolve => {
          const stop = jest.fn(() => resolve());
          waiters.push(stop);
          signal.addEventListener('abort', stop, { once: true });
          ready(true);
        });
      });
    }
    jobs.start();
    jobs.start();
    runtime.forEach((run, index) => expect(jest.mocked(run).mock.calls.map(call => call[0])).toEqual(expectedModes[index]));
    expect(new BackgroundJobsHealthController(jobs).status().status).toBe('ok');
    await jobs.beforeApplicationShutdown();
    expect(waiters.length).toBe(expectedModes.flat().length);
    for (const stop of waiters) expect(stop).toHaveBeenCalledTimes(1);
    expect(() => new BackgroundJobsHealthController(jobs).status()).toThrow();
  });
});
