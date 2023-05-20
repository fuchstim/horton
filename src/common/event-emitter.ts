import BaseEventEmitter from 'events';

type TEventTypeMap = Record<string, string | number | object | null>;
type TEventName<T extends TEventTypeMap> = string & keyof T;
type TEventListener<T> = (payload: T) => void | Promise<void>;

type TBuiltinEvents = {
  error: Error,
  newListener: string,
  removeListener: string
};

export class TypedEventEmitter<T extends TEventTypeMap> extends BaseEventEmitter {
  constructor(options?: { captureRejections?: boolean }) {
    super({ ...options, captureRejections: true, });
  }

  override on<N extends TEventName<T>>(eventName: N, listener: TEventListener<T[N]>) {
    return super.on(eventName, listener);
  }

  override off<N extends TEventName<T>>(eventName: N, listener: TEventListener<T[N]>) {
    return super.off(eventName, listener);
  }

  override emit<N extends TEventName<T>>(eventName: N, payload: T[N]) {
    return super.emit(eventName, payload);
  }

  async emitSync<N extends TEventName<T>>(eventName: N, payload: T[N]) {
    const listeners = this.rawListeners(eventName);

    await Promise.all(
      listeners.map(
        listener => Promise
          .resolve(listener.apply(listener, [ payload, ]))
          .catch(() => null)
      )
    );
  }

  override addListener<
    N extends TEventName<T>
  >(eventName: N, listener: TEventListener<T[N]>) {
    return super.addListener(eventName, listener);
  }

  override once<N extends TEventName<T>>(eventName: N, listener: TEventListener<T[N]>) {
    return super.once(eventName, listener);
  }

  override removeListener<
    N extends TEventName<T>
  >(eventName: N, listener: TEventListener<T[N]>) {
    return super.removeListener(eventName, listener);
  }

  override removeAllListeners<N extends TEventName<T>>(eventName: N) {
    return super.removeAllListeners(eventName);
  }

  override listeners<N extends TEventName<T>>(eventName: N): TEventListener<T[N]>[] {
    return super.listeners(eventName) as TEventListener<T[N]>[];
  }

  override rawListeners<N extends TEventName<T>>(eventName: N): TEventListener<T[N]>[] {
    return super.rawListeners(eventName) as TEventListener<T[N]>[];
  }

  override listenerCount<N extends TEventName<T>>(eventName: N): number {
    return super.listenerCount(eventName);
  }

  override prependListener<
    N extends TEventName<T>
  >(eventName: N, listener: TEventListener<T[N]>) {
    return super.prependListener(eventName, listener);
  }

  override prependOnceListener<
    N extends TEventName<T>
  >(eventName: N, listener: TEventListener<T[N]>) {
    return super.prependOnceListener(eventName, listener);
  }

  override eventNames() {
    return super.eventNames() as unknown as TEventName<T>[];
  }
}

export default class EventEmitter<T extends TEventTypeMap> extends TypedEventEmitter<T & TBuiltinEvents> {}
