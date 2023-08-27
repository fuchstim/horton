import Logger from './common/logger';
const logger = Logger.ns('LivenessChecker');

import type EventQueue from './_event-queue';

import { EInternalOperation, TInternalQueueRow, TLivenessCheckerEvents, TLivenessCheckerOptions, TLivenessCheckerStatus } from './common/types';
import { TypedEventEmitter } from './common/typed-event-emitter';


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

  get status(): TLivenessCheckerStatus {
    const isHealthy = this.lastHeartbeat.getTime() > Date.now() - (this.pulseIntervalMs * this.maxMissedPulses);
    const isDead = this.lastHeartbeat.getTime() > Date.now() - (this.pulseIntervalMs * this.maxMissedPulses * 3);

    const status = isHealthy ? 'healthy' : 'unhealthy';

    return isDead ? 'dead' : status;
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

    logger.debug(`Status: ${this.status}`);

    this.emit(this.status, { lastHeartbeatAt: this.lastHeartbeat, });
  }

  private async handleQueueNotification(rowId: number) {
    await this.eventQueue.dequeue<TInternalQueueRow, void>(
      rowId,
      row => {
        const pulsedAt = row.queuedAt;
        const pulseLag = new Date().getTime() - pulsedAt.getTime();
        logger.debug(`Received pulse from ${pulseLag / 1_000}s ago`);

        this.emit('heartbeat', { pulsedAt, pulseLag, });

        if (pulsedAt.getTime() > this.lastHeartbeat.getTime()) {
          this.lastHeartbeat = pulsedAt;
        }
      }
    );
  }
}
