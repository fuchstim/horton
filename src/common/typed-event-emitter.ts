import EventEmitter from "events"
import TypedEmitter, {EventMap} from "typed-emitter"

type TEventTypeMap = Record<string, string | number | object | null>;
type TEventName<T extends TEventTypeMap> = string & keyof T;
type TEventListener<T> = (payload: T) => void | Promise<void>;

type TEventHandlers<E extends TEventTypeMap> = {
  [K in keyof E]: TEventListener<E[K]>;
};

type EventEmitterOptions = { captureRejections?: boolean };

export class TypedEventEmitter<T extends TEventTypeMap> extends (EventEmitter as { new<T extends EventMap>(options?: EventEmitterOptions): TypedEmitter<T> })<TEventHandlers<T>> {
  constructor(options: EventEmitterOptions = {}) {
    super(options);
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
}
