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

  getKey(model: string): string {
    return model[0].toLowerCase() + model.slice(1);
  }

  setElement(element: { [name: string]: BaseModelDefault }) {
    for (const key in element) {
      if (Object.prototype.hasOwnProperty.call(element, key)) {
        const e = element[key];
        const newKey = this.getKey(key);
        if (newKey !== key) {
          element[newKey] = e;
          // @ts-ignore
          element[key] = undefined;
          delete element[key];
        }
      }
    }
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

  private async makePromise(
    input: IInput<unknown, unknown>,
    output: any,
    selected?: number
  ): Promise<IOutput<unknown, unknown, unknown>> {
    const key = this.getKey(input.scheme) || input.scheme;
    let hits =
      output.body?.hits?.hits || output.body?.items || output.body || output;
    hits = hits?.map?.((value) => {
      return {
        ...value,
        _index: input.scheme,
        _source:
          this.element[key]?.reverseParse(
            value?._source?.script || value?._source,
            undefined,
            selected
          ) || value?._source,
      };
    }) || {
      ...hits,
      _index: input.scheme,
      _source:
        this.element[key]?.reverseParse(
          hits?._source?.script || hits?._source,
          undefined,
          selected
        ) || hits?._source,
    };
    const result = input.single ? (Array.isArray(hits) ? hits[0] : hits) : hits;
    const r = await {
      receivedItem: result,
      result,
      selectedItem: input.selectedItem,
      sentItem: input.item,
    };
    // console.log(r);
    return r as unknown as Promise<IOutput<unknown, unknown, unknown>>;
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

  toBulk(
    scheme: string,
    input: any[],
    selectedInput: any[],
    type: string,
    // eslint-disable-next-line no-unused-vars, @typescript-eslint/no-unused-vars
    options?: { page?: number; pageSize?: number },
    index?: number
  ): { body: any[] } {
    // TODO: from/size and query
    // headers page and pageSize/pagesize
    const key = this.getKey(scheme) || scheme;
    const body: any[] = [];
    // TODO: check if its working for create, update and delete
    for (const i of input) {
      const method = {};
      method[type] = {
        _index: this.element[key]?.getName(index) || key,
        _type: '_doc',
        _id: this.generateId(scheme, i, index, options),
        // _id: i._id,
      };
      delete i._index;
      delete i._type;
      body.push(method, i);
    }
    // console.log('Bulk body:', body);
    return { body };
  }

  constAddRange(query, key, element, within = 'must') {
    if (
      key.includes('.$gt') ||
      key.includes('.$gte') ||
      key.includes('.$lt') ||
      key.includes('.$lte')
    ) {
      const currentKey = key
        .replace('.$gte', '')
        .replace('.$gt', '')
        .replace('.$lte', '')
        .replace('.$lt', '');
      let currentRange = query[within].find(
        (value) => value?.range?.[currentKey]
      );
      if (!currentRange) {
        currentRange = { range: {} };
        currentRange.range[currentKey] = {};
        query[within].push(currentRange);
      }
      if (key.includes('.$gte')) {
        currentRange.range[currentKey].gte = element;
      } else if (key.includes('.$gt')) {
        currentRange.range[currentKey].gt = element;
      } else if (key.includes('.$lte')) {
        currentRange.range[currentKey].lte = element;
      } else if (key.includes('.$lt')) {
        currentRange.range[currentKey].lt = element;
      }
    }
  }

  addTerms(query, key, element, within = 'must') {
    if (Array.isArray(element)) {
      const elementWithKey = {};
      elementWithKey[key.replace('.$in', '')] = element;
      const t = { terms: elementWithKey };
      query[within].push(t);
    } else {
      if (
        key.includes('.$gt') ||
        key.includes('.$gte') ||
        key.includes('.$lt') ||
        key.includes('.$lte')
      ) {
        this.constAddRange(query, key, element, 'must');
      } else if (key.includes('.$regex') || key.includes('.$wildcard')) {
        const e = element;
        try {
          element = JSON.parse(e);
        } catch (error) {
          element = e;
        }
        const elementWithKey = {};
        elementWithKey[key.replace('.$regex', '').replace('.$wildcard', '')] =
          element;
        const v =
          typeof elementWithKey == 'object'
            ? elementWithKey
            : { value: elementWithKey };
        const t = key.includes('.$wildcard') ? { wildcard: v } : { regexp: v };
        query[within].push(t);
      } else {
        const elementWithKey = {};
        elementWithKey[key] = element;
        const t = { match: elementWithKey };
        query[within].push(t);
      }
    }
  }

  toBoolQuery(input: any): any {
    const query: any = {};
    for (const key in input) {
      if (Object.prototype.hasOwnProperty.call(input, key)) {
        const element = input[key];
        if (key.includes('.$ne') || key.includes('.$nin')) {
          query.must_not = query.must_not || [];
          this.addTerms(
            query,
            key.replace('.$ne', '').replace('.$nin', ''),
            element,
            'must_not'
          );
        } else {
          query.must = query.must || [];
          this.addTerms(query, key, element, 'must');
        }
      }
    }
    if (query.must?.length === 0) delete query.must;
    if (query.must_not?.length === 0) delete query.must_not;
    // console.log('Query:');
    // if (query.must) query.must.forEach((value) => console.log(value));
    // if (query.must_not) query.must_not.forEach((value) => console.log(value));
    // console.log('Query end.');
    return query;
  }

  elementToPainless(object) {
    // ['customer': 'Joe', 'group': 'blah', 'name': 'abc']
    const painless: string[] = [];

    if (object)
      if (typeof object === 'object')
        for (const key in object) {
          if (Object.prototype.hasOwnProperty.call(object, key)) {
            const element = object[key];
            if (typeof element === 'object') {
              painless.push(`'${key}'` + ':' + this.elementToPainless(element));
            } else {
              painless.push(`'${key}'` + ':' + JSON.stringify(element));
            }
          }
        }
      else return JSON.stringify(object);
    else return JSON.stringify(object);
    return '[' + painless.join(',') + ']';
  }

  arrayToPainless(array) {
    // [X, Y]
    const painless: string[] = [];
    if (array == undefined) return JSON.stringify(array);
    if (array)
      for (const o of array) {
        if (Array.isArray(o)) {
          painless.push(this.arrayToPainless(o));
        } else {
          painless.push(this.elementToPainless(o));
        }
      }
    return '[' + painless.join(',') + ']';
  }

  jsonToPainless(json, level = 'ctx._source') {
    const painless: string[] = [];
    if (!json) return painless.join('');
    for (const key in json) {
      if (Object.prototype.hasOwnProperty.call(json, key)) {
        const element = json[key];
        if (!Array.isArray(element)) {
          painless.push(
            level + '.' + key + '=' + this.elementToPainless(element) + ';'
          );
        } else {
          painless.push(
            level + '.' + key + `= ${this.arrayToPainless(element)};`
          );
        }
      }
    }
    return painless.join('');
  }

  generateId(
    model: string,
    item,
    index?: number,
    options?: { page?: number; pageSize?: number; id?: any }
  ) {
    let id = undefined;
    if (options?.id) {
      const o = {};
      o[options.id] = 0;
      const p = this.parse(model, o, index);
      for (const key in p) {
        if (Object.hasOwnProperty.call(p, key)) {
          id = item[key];
        }
      }
    }
    return id;
  }
  toBody(
    model: string,
    input: any,
    selectedInput?: any,
    options?: { page?: number; pageSize?: number; id?: any },
    index?: number
  ): any {
    // TODO: from/size and query
    // headers page and pageSize/pagesize
    const key = this.getKey(model) || model;
    const type = input._type || this.element[key]?.getType() || '_doc';
    delete input._type;
    // console.log('selectedInput:', selectedInput);
    const i = input && Object.keys(input).length > 0 ? input : undefined;
    const id = this.generateId(model, i, index, options);
    const painless = this.jsonToPainless(i);

    const body = {
      index: this.element[key]?.getName(index) || key,
      type: type,
      refresh: painless ? true : undefined,
      id,
      body:
        selectedInput && Object.keys(selectedInput).length > 0
          ? {
              script: painless
                ? { source: painless }
                : i && Object.keys(i).length > 0
                ? i
                : undefined,
              query: { bool: this.toBoolQuery(selectedInput) },
            }
          : input,
    };
    if (options?.pageSize) {
      body['from'] = (options.page || 0) * options.pageSize;
      body['size'] = options.pageSize;
    }
    // console.log('Body:', body);
    return body;
  }

  parse(model: string, input: any, index?: number): any {
    const key = this.getKey(model) || model;
    const p = this.element[key]
      ? this.element[key].parse(input, undefined, index)
      : input;
    // console.log('PARSE:', p);
    return p;
  }

  reverseParse(model: string, input: any, index?: number): any {
    const key = this.getKey(model) || model;
    const p = this.element[key]
      ? this.element[key].reverseParse(input, undefined, index)
      : input;
    // console.log('REPARSE:', p);
    return p;
  }

  getSelected(input: IInput<any, any>) {
    const key = this.getKey(input.scheme) || input.scheme;
    const element = this.element[key];
    const selector = element.getSelector();
    const selected: number | undefined = selector
      ? (input.selectedItem as any)?.[selector] || 0
      : undefined;
    if (selector) delete input.selectedItem?.[selector];
    return selected;
  }

  async create(
    input: IInputCreate<unknown>,
    // eslint-disable-next-line no-unused-vars, @typescript-eslint/no-unused-vars
    _transaction?: ITransaction
  ): Promise<IOutput<unknown, unknown, unknown>> {
    // console.log('CREATE:', input);

    const selected = this.getSelected(input);

    return Array.isArray(input.item)
      ? this.makePromise(
          input,
          // @ts-ignore
          await this.client.bulk(
            this.toBulk(
              input.scheme,
              input.item.map((i) => this.parse(input.scheme, i, selected)),
              this.parse(input.scheme, input.selectedItem, selected),
              'index',
              this.generatePageOptions(input),
              selected
            )
          ),
          selected
        )
      : this.makePromise(
          input,
          // @ts-ignore
          await this.client.index(
            this.toBody(
              input.scheme,
              this.parse(input.scheme, input.item, selected),
              this.parse(input.scheme, input.selectedItem, selected),
              this.generatePageOptions(input),
              selected
            )
          ),
          selected
        );
  }

  generatePageOptions(
    input: IInputCreate | IInputRead | IInputUpdate | IInputDelete
  ): { page?: number; pageSize?: number } {
    const options =
      input.eventOptions || input.options || input.additionalOptions || {};
    options.pageSize = options.pageSize || options.pagesize;
    options.page = options.page || options.pageNumber || options.pagenumber;
    if (options.pageSize) options.pageSize = Number(options.pageSize);
    if (options.page) options.page = Number(options.page);
    if (options.pageSize && !options.page) options.page = 0;
    return options;
  }

  async read(
    input: IInputRead,
    // eslint-disable-next-line no-unused-vars, @typescript-eslint/no-unused-vars
    _transaction?: ITransaction
  ): Promise<IOutput<unknown, unknown, unknown>> {
    // console.log('read', input);
    const selected = this.getSelected(input);
    return Array.isArray(input.item)
      ? this.makePromise(
          input,
          // @ts-ignore
          await this.client.search(
            this.toBody(
              input.scheme,
              this.parse(input.scheme, input.item, selected),
              this.parse(input.scheme, input.selectedItem, selected),
              this.generatePageOptions(input),
              selected
            )
          ),
          selected
        )
      : this.makePromise(
          input,
          // @ts-ignore
          await this.client.search(
            this.toBody(
              input.scheme,
              this.parse(input.scheme, input.item, selected),
              this.parse(input.scheme, input.selectedItem, selected),
              this.generatePageOptions(input),
              selected
            )
          ),
          selected
        );
  }
  async update(
    input: IInputUpdate<unknown>,
    // eslint-disable-next-line no-unused-vars, @typescript-eslint/no-unused-vars
    _transaction?: ITransaction
  ): Promise<IOutput<unknown, unknown, unknown>> {
    const selected = this.getSelected(input);
    return Array.isArray(input.item)
      ? this.makePromise(
          input,
          // @ts-ignore
          await this.client.bulk(
            this.toBulk(
              input.scheme,
              input.item.map((i) => this.parse(input.scheme, i, selected)),
              this.parse(input.scheme, input.selectedItem, selected),
              'update',
              this.generatePageOptions(input),
              selected
            )
          ),
          selected
        )
      : this.makePromise(
          input,
          // @ts-ignore
          await this.client.updateByQuery(
            this.toBody(
              input.scheme,
              this.parse(input.scheme, input.item, selected),
              this.parse(input.scheme, input.selectedItem, selected),
              this.generatePageOptions(input),
              selected
            )
          ),
          selected
        );
  }
  async delete(
    input: IInputDelete,
    // eslint-disable-next-line no-unused-vars, @typescript-eslint/no-unused-vars
    _transaction?: ITransaction
  ): Promise<IOutput<unknown, unknown, unknown>> {
    // console.log('FUCKING DELETE');
    const selected = this.getSelected(input);

    return Array.isArray(input.item)
      ? this.makePromise(
          input,
          // @ts-ignore
          await this.client.bulk(
            this.toBulk(
              input.scheme,
              input.item.map((i) => this.parse(input.scheme, i, selected)),
              this.parse(input.scheme, input.selectedItem, selected),
              'delete',
              this.generatePageOptions(input),
              selected
            )
          ),
          selected
        )
      : this.makePromise(
          input,
          // @ts-ignore
          await this.client.deleteByQuery(
            this.toBody(
              input.scheme,
              this.parse(input.scheme, input.item, selected),
              this.parse(input.scheme, input.selectedItem, selected),
              this.generatePageOptions(input),
              selected
            )
          ),
          selected
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
