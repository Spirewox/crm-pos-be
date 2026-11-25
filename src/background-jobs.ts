import { BeforeApplicationShutdown, Controller, Get, Injectable, Logger, ServiceUnavailableException } from '@nestjs/common';
import { ModuleRef } from '@nestjs/core';

import { getConnectionToken } from '@nestjs/mongoose';
import type { Connection } from 'mongoose';
import { runPosEvents } from './events/main';
import { runPosPayments } from './events/payment-main';
import { bullmqReconciliationEnabled } from './pos/checkout-config';
import { CheckoutService } from './pos/checkout.service';

type JobState = 'starting' | 'ready' | 'unavailable' | 'stopped' | 'failed';
type RunJob = (signal: AbortSignal, setReady: (ready: boolean) => void) => Promise<void>;

@Injectable()
export class BackgroundJobs implements BeforeApplicationShutdown {
  private readonly logger = new Logger(BackgroundJobs.name);
  private readonly stop = new AbortController();
  private readonly states = new Map<string, JobState>();
  private readonly tasks: Promise<void>[] = [];

  constructor(private readonly modules: ModuleRef) {}

  start() {
    if (this.tasks.length || this.stop.signal.aborted) return;
    const events = process.env.POS_EMBED_EVENTS === 'true';
    const payments = process.env.POS_EMBED_PAYMENTS === 'true' &&
      bullmqReconciliationEnabled() && process.env.POS_PAYMENT_WORKER_ENABLED !== 'false';
    if (!events && !payments) return;
    const url = process.env.EVENT_REDIS_URL;
    if (!url) throw new Error('EVENT_REDIS_URL is required for embedded background jobs');

    const db = this.modules.get<Connection>(getConnectionToken(), { strict: false });
    if (events) {
      for (const mode of ['relay', 'worker'] as const) {
        this.launch('pos-events-' + mode, (signal, ready) => runPosEvents(mode, db, url, signal, ready));
      }
    }
    if (payments) {
      const service = this.modules.get(CheckoutService, { strict: false });
      for (const mode of ['relay', 'worker'] as const) {
        this.launch('pos-payments-' + mode, (signal, ready) => runPosPayments(mode, db, service, url, signal, ready));
      }
    }
  }

  private launch(name: string, run: RunJob) {
    this.states.set(name, 'starting');
    const task = run(this.stop.signal, ready => {
      if (!this.stop.signal.aborted) this.states.set(name, ready ? 'ready' : 'unavailable');
    }).then(() => {
      const stopped = this.stop.signal.aborted;
      this.states.set(name, stopped ? 'stopped' : 'failed');
      if (!stopped) this.logger.error(`${name} exited unexpectedly`);
    }).catch(() => {
      this.states.set(name, 'failed');
      this.logger.error(`${name} stopped with an error`);
    });
    this.tasks.push(task);
  }

  async beforeApplicationShutdown() {
    this.stop.abort();
    await Promise.all(this.tasks);
  }

  health() {
    const jobs = Object.fromEntries(this.states);
    return {
      status: [...this.states.values()].every(state => state === 'ready') ? 'ok' : 'degraded',
      jobs,
    };
  }
}

@Controller('health/background-jobs')
export class BackgroundJobsHealthController {
  constructor(private readonly jobs: BackgroundJobs) {}

  @Get()
  status() {
    const report = this.jobs.health();
    if (report.status === 'degraded') throw new ServiceUnavailableException(report);
    return report;
  }
}
