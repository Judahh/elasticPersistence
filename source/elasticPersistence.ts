/* eslint-disable @typescript-eslint/ban-ts-comment */
/* eslint-disable @typescript-eslint/explicit-module-boundary-types */
// file deepcode ignore object-literal-shorthand: annoying
/* eslint-disable @typescript-eslint/no-explicit-any */
// file deepcode ignore no-any: any needed
import {
  IPersistence,
  PersistenceInfo,
  IOutput,
  // RelationValueDAODB,
  // SelectedItemValue,
  IInputCreate,
  IInputUpdate,
  IInputRead,
  IInputDelete,
  IInput,
  ITransaction,
} from 'flexiblepersistence';
import { Client as Client6 } from 'es6';
import { Client as Client7 } from 'es7';
import { Client as Client8 } from 'es8';
import BaseModelDefault from './baseModelDefault';
import { ElasticPersistenceInfo } from './elasticPersistenceInfo';
import { Transaction } from './transaction';

const version = (process.env.ES_VERSION || '8').split('.')[0].trim();

const Client = version === '6' ? Client6 : version === '7' ? Client7 : Client8;

export class ElasticPersistence implements IPersistence {
  private persistenceInfo: ElasticPersistenceInfo;
  private client: Client6 | Client7 | Client8;

  element: {
    [name: string]: BaseModelDefault;
  } = {};

  constructor(
    persistenceInfo: ElasticPersistenceInfo,
    element?: {
      [name: string]: BaseModelDefault;
    }
  ) {
    this.persistenceInfo = persistenceInfo;
    if (this.persistenceInfo.elasticOptions) {
      this.client = new Client(this.persistenceInfo.elasticOptions as any);
    } else throw new Error('Elastic Options nonexistent.');
    if (element) this.setElement(element);
  }
  async transaction(
    // eslint-disable-next-line no-unused-vars, @typescript-eslint/no-unused-vars
    options?: any,
    // eslint-disable-next-line no-unused-vars
    callback?: (transaction: ITransaction) => Promise<void>
  ): Promise<ITransaction> {
    const t = new Transaction(this.client);
    await t.begin(options);
    await callback?.(t);
    await t.commit();
    return t;
  }
  clear(transaction?: ITransaction): Promise<boolean> {
    return new Promise<boolean>(async (resolve, reject) => {
      try {
        if (transaction) {
          await (transaction as Transaction).append({
            delete: {
              index: '_all',
            },
          });
        } else {
          // @ts-ignore
          await this.client.indices.delete({ index: '_all' });
        }
        resolve(true);
      } catch (error) {
        reject(error);
      }
    });
  }

  setElement(element: { [name: string]: BaseModelDefault }) {
    this.element = element;
  }

  protected aggregateFromReceivedArray(realInput: any[]): any[] {
    return realInput.map((value) => this.aggregateFromReceived(value));
  }

  protected aggregateFromReceived(value): any {
    if (value.id)
      return {
        ...value,
        id: value.id.toString(),
      };
    return value;
  }

  protected realInput(input: IInput<unknown, unknown>) {
    // console.log(input);

    let realInput = input.item ? input.item : {};
    if (realInput)
      if (Array.isArray(realInput))
        realInput = this.aggregateFromReceivedArray(realInput);
      else realInput = this.aggregateFromReceived(realInput);

    // console.log(realInput);
    return realInput;
  }

  private makePromise(
    input: IInput<unknown, unknown>,
    output: any
  ): Promise<IOutput<unknown, unknown, unknown>> {
    return output;
  }
  other(
    input: IInput<unknown, unknown>
  ): Promise<IOutput<unknown, unknown, unknown>> {
    return new Promise<IOutput<unknown, unknown, unknown>>((resolve) => {
      resolve({
        receivedItem: input,
      });
    });
  }

  toBulk(scheme: string, input: any[], type: string): any[] {
    const body: any[] = [];
    for (const i of input) {
      const method = {};
      method[type] = {
        _index: scheme,
        _type: '_doc',
        // _id: i._id,
      };
      body.push(method, i);
    }
    return body;
  }

  parse(model: string, input: any): any {
    this.element[model] ? this.element[model].parse(input) : input;
  }

  async create(
    input: IInputCreate<unknown>,
    // eslint-disable-next-line no-unused-vars, @typescript-eslint/no-unused-vars
    _transaction?: ITransaction
  ): Promise<IOutput<unknown, unknown, unknown>> {
    // console.log('CREATE:', input);

    return Array.isArray(input.item)
      ? this.makePromise(
          input,
          // @ts-ignore
          await this.client.bulk(
            this.toBulk(
              input.scheme,
              input.item.map((i) => this.parse(input.scheme, i)),
              'index'
            )
          )
        )
      : this.makePromise(
          input,
          // @ts-ignore
          await this.client.index(this.parse(input.scheme, input.item))
        );
  }
  read(
    input: IInputRead,
    // eslint-disable-next-line no-unused-vars, @typescript-eslint/no-unused-vars
    _transaction?: ITransaction
  ): Promise<IOutput<unknown, unknown, unknown>> {
    // console.log('read', input);
    return Array.isArray(input.item)
      ? this.makePromise(
          input,
          // @ts-ignore
          await this.client.search(this.parse(input.scheme, input.item))
        )
      : this.makePromise(
          input,
          // @ts-ignore
          await this.client.search(this.parse(input.scheme, input.item))
        )[0];
  }
  update(
    input: IInputUpdate<unknown>,
    // eslint-disable-next-line no-unused-vars, @typescript-eslint/no-unused-vars
    _transaction?: ITransaction
  ): Promise<IOutput<unknown, unknown, unknown>> {
    return Array.isArray(input.item)
      ? this.makePromise(
          input,
          // @ts-ignore
          await this.client.bulk(
            this.toBulk(
              input.scheme,
              input.item.map((i) => this.parse(input.scheme, i)),
              'update'
            )
          )
        )
      : this.makePromise(
          input,
          // @ts-ignore
          await this.client.update(this.parse(input.scheme, input.item))
        );
  }
  delete(
    input: IInputDelete,
    // eslint-disable-next-line no-unused-vars, @typescript-eslint/no-unused-vars
    _transaction?: ITransaction
  ): Promise<IOutput<unknown, unknown, unknown>> {
    // console.log('FUCKING DELETE');

    return Array.isArray(input.item)
      ? this.makePromise(
          input,
          // @ts-ignore
          await this.client.bulk(
            this.toBulk(
              input.scheme,
              input.item.map((i) => this.parse(input.scheme, i)),
              'delete'
            )
          )
        )
      : this.makePromise(
          input,
          // @ts-ignore
          await this.client.delete(this.parse(input.scheme, input.item))
        );
  }

  getPersistenceInfo(): PersistenceInfo {
    return this.persistenceInfo;
  }

  // eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
  getClient() {
    return this.client;
  }

  close(): Promise<boolean> {
    return this.client.close() as Promise<unknown> as Promise<boolean>;
  }
}
