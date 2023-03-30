import { Default, IDefault } from '@flexiblepersistence/default-initializer';
export default class BaseModelDefault extends Default {
  protected strict = false;
  protected attributes = {};
  protected aliasFields = {};

  generateType() {
    this.setType('_doc');
  }

  getAttributes() {
    return this.attributes;
  }

  getAliasFields() {
    return this.aliasFields;
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

  reverseParse(input: any, aliasFields = this.aliasFields): any {
    const newAliasFields = this.reverseAliasFields(aliasFields);
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

  parse(input: any, aliasFields = this.aliasFields): any {
    let output;
    if (Array.isArray(input)) {
      output = [];
      for (let index = 0; index < input.length; index++) {
        const element = input[index];
        output[index] = this.parse(element, aliasFields);
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
              !['$gte', '$lte', '$gt', '$lt', '$ne', '$nin', '$in'].includes(
                cur
              )
            ) {
              acc += '.' + cur;
            }
            return acc;
          }, '');
          const firstFilter = splitFilters.find((f) =>
            ['$gte', '$lte', '$gt', '$lt', '$ne', '$nin', '$in'].includes(f)
          );
          let newKey = this.getNestedKey(noFilterKey, aliasFields);
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
