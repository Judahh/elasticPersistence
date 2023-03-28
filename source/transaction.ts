import { ITransaction } from 'flexiblepersistence';
import { Client as Client6 } from 'es6';
import { Client as Client7 } from 'es7';
import {
  Client as Client8,
  TransportRequestOptions as TransportRequestOptions8,
} from 'es8';
import { TransportRequestOptions as TransportRequestOptions6 } from 'es6/lib/Transport';
import { TransportRequestOptions as TransportRequestOptions7 } from 'es7/lib/Transport';

export class Transaction implements ITransaction {
  private options?:
    | TransportRequestOptions6
    | TransportRequestOptions7
    | TransportRequestOptions8;
  private client: Client6 | Client7 | Client8;
  constructor(client: Client6 | Client7 | Client8) {
    this.client = client;
  }
  private body: any[] = [];
  async begin(
    options?:
      | TransportRequestOptions6
      | TransportRequestOptions7
      | TransportRequestOptions8
  ): Promise<void> {
    this.options = options;
  }
  async commit(): Promise<void> {
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    return await this.client?.bulk(
      { body: this.body } as any,
      this.options as any
    );
  }
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  async rollback(): Promise<void> {}

  async append(body: any): Promise<void> {
    this.body.push(body);
  }
}
