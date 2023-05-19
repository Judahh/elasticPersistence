import { Default, IDefault } from '@flexiblepersistence/default-initializer';
export default class BaseModelDefault extends Default {
  protected strict = false;
  protected attributes: any | Array<any> = {};
  protected aliasFields: any | Array<any> = {};
  protected selector?: string;
  protected names?: Array<string>;

  getSelector() {
    return this.selector;
  }

  getName(index?: number): string {
    return Array.isArray(this.attributes) && index != undefined && this.names
      ? this.names[index]
      : this.name;
  }

  generateType() {
    this.setType('_doc');
  }

  getAttributes(index?: number) {
    return Array.isArray(this.attributes) && index != undefined
      ? this.attributes[index]
      : this.attributes;
  }

  getAliasFields(index?: number, aliasFields = this.aliasFields) {
    return Array.isArray(aliasFields) && index != undefined
      ? aliasFields[index]
      : aliasFields;
  }

  reverseAliasFields(aliasFields = this.aliasFields): any {
    const newAliasFields = {};
    for (const key in aliasFields) {
      if (Object.prototype.hasOwnProperty.call(aliasFields, key)) {
        const element = aliasFields[key];
        if (Array.isArray(element)) {
          newAliasFields[element[0]] = [
            key,
            this.reverseAliasFields(element[1]),
          ];
        } else {
          newAliasFields[element] = key;
        }
      }
    }
    return newAliasFields;
  }

  reverseParse(
    input: any,
    aliasFields = this.aliasFields,
    index?: number
  ): any {
    const newAliasFields = this.reverseAliasFields(
      this.getAliasFields(index, aliasFields)
    );
    return this.parse(input, newAliasFields);
  }

  getNestedKey(key: string, aliasFields = this.aliasFields): string {
    // get nested key
    const keys = key.split('.');
    return (
      keys.reduce((acc, cur) => {
        if (acc) {
          return acc[cur];
        } else {
          return aliasFields[cur] || aliasFields || cur;
        }
      }, aliasFields) ||
      aliasFields[key] ||
      key
    );
  }

  parse(input: any, aliasFields = this.aliasFields, index?: number): any {
    let output;
    if (Array.isArray(input)) {
      output = [];
      for (let aIndex = 0; aIndex < input.length; aIndex++) {
        const element = input[aIndex];
        output[aIndex] = this.parse(
          element,
          this.getAliasFields(index, aliasFields)
        );
      }
    } else {
      output = {};
      for (const key in input) {
        if (Object.prototype.hasOwnProperty.call(input, key)) {
          let element = input[key];
          const splitFilters = key.split('.');
          // join if filters if they are not $gte, $lte, $gt, $lt, $ne, $nin, $in
          const noFilterKey = splitFilters.reduce((acc, cur) => {
            acc = acc || '';
            if (
              ![
                '$gte',
                '$lte',
                '$gt',
                '$lt',
                '$ne',
                '$nin',
                '$in',
                '$regex',
                '$wildcard',
              ].includes(cur)
            ) {
              acc = acc ? '.' + cur : cur;
            }
            return acc;
          }, '');
          const firstFilter = splitFilters.find((f) =>
            [
              '$gte',
              '$lte',
              '$gt',
              '$lt',
              '$ne',
              '$nin',
              '$in',
              '$regex',
              '$wildcard',
            ].includes(f)
          );
          let newKey = this.getNestedKey(
            noFilterKey,
            this.getAliasFields(index, aliasFields)
          );
          newKey = newKey || key;
          if (firstFilter) {
            newKey += '.' + firstFilter;
          }
          if (Array.isArray(newKey)) {
            element = this.parse(element, newKey[1]);
            newKey = newKey[0];
          }
          output[newKey] = element;
          if (this.strict) {
            if (!this.attributes[newKey]) {
              delete output[newKey];
            }
          }
        }
      }
    }
    return output;
  }

  constructor(initDefault?: IDefault) {
    super(initDefault);
  }
  init(initDefault?: IDefault): void {
    // console.log('init:', initDefault);

    super.init(initDefault);
  }
}
