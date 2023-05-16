import Logger from './common/logger';
const logger = Logger.ns('LivenessChecker');

import type EventQueue from './_event-queue';

import { EInternalOperation, TInternalQueueRow } from './common/types';

export default class LivenessChecker {
  private eventQueue: EventQueue;
  private intervalMs: number;
  private intervalTimer?: NodeJS.Timer;

  constructor(eventQueue: EventQueue, intervalMs: number) {
    this.eventQueue = eventQueue;
    this.intervalMs = intervalMs;

    this.eventQueue.on(
      `internal:${EInternalOperation.LIVENESS_PULSE}`,
      rowId => this.handleQueueNotification(rowId)
    );
  }

  start() {
    logger.debug(`Starting liveness checker every ${this.intervalMs / 1_000}s`);

    this.intervalTimer = setInterval(
      () => this.pulse(),
      this.intervalMs
    );
  }

  stop() {
    logger.debug('Stopping...');

    clearInterval(this.intervalTimer);
  }

  private async pulse() {
    logger.debug('Sending pulse...');

    await this.eventQueue.queueInternal(EInternalOperation.LIVENESS_PULSE);
  }

  private async handleQueueNotification(rowId: number) {
    await this.eventQueue.dequeue<TInternalQueueRow, void>(
      rowId,
      row => {
        const pulseAge = new Date().getTime() - row.queuedAt.getTime();

        logger.debug(`Received pulse from ${pulseAge / 1_000}s ago`);
      }
    );
  }
}
