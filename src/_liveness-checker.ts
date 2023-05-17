import Logger from './common/logger';
const logger = Logger.ns('LivenessChecker');

import type EventQueue from './_event-queue';

import { EInternalOperation, TInternalQueueRow, TLivenessCheckerOptions } from './common/types';
import { TypedEventEmitter } from './common/event-emitter';

type TLivenessCheckerEvents = {
  healthy: null,
  unhealthy: null,
  heartbeat: number
};

export default class LivenessChecker extends TypedEventEmitter<TLivenessCheckerEvents> {
  private readonly eventQueue: EventQueue;
  private readonly pulseIntervalMs: number;
  private readonly maxMissedPulses: number;

  private intervalTimer?: NodeJS.Timer;
  private lastHeartbeat: Date = new Date();

  constructor(eventQueue: EventQueue, options?: TLivenessCheckerOptions) {
    super();

    this.eventQueue = eventQueue;
    this.pulseIntervalMs = options?.pulseIntervalMs ?? 10_000;
    this.maxMissedPulses = options?.maxMissedPulses ?? 3;

    this.eventQueue.on(
      `internal:${EInternalOperation.LIVENESS_PULSE}`,
      rowId => this.handleQueueNotification(rowId)
    );
  }

  start() {
    logger.debug(`Starting liveness checker every ${this.pulseIntervalMs / 1_000}s`);

    this.lastHeartbeat = new Date();

    this.intervalTimer = setInterval(
      () => this.pulse(),
      this.pulseIntervalMs
    );
  }

  stop() {
    logger.debug('Stopping...');

    clearInterval(this.intervalTimer);
  }

  private async pulse() {
    logger.debug('Sending pulse...');

    await this.eventQueue.queueInternal(EInternalOperation.LIVENESS_PULSE);

    const isHealthy = this.lastHeartbeat.getTime() > Date.now() - (this.pulseIntervalMs * this.maxMissedPulses);
    const status = isHealthy ? 'healthy' : 'unhealthy';

    logger.debug(`Status: ${status}`);
    this.emit(status, null);
  }

  private async handleQueueNotification(rowId: number) {
    await this.eventQueue.dequeue<TInternalQueueRow, void>(
      rowId,
      row => {
        const pulseLag = new Date().getTime() - row.queuedAt.getTime();
        logger.debug(`Received pulse from ${pulseLag / 1_000}s ago`);

        this.emit('heartbeat', pulseLag);

        if (row.queuedAt.getTime() > this.lastHeartbeat.getTime()) {
          this.lastHeartbeat = row.queuedAt;
        }
      }
    );
  }
}
