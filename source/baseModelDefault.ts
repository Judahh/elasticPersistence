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

  parse(input: any): any {
    const output = {};
    for (const key in input) {
      if (Object.prototype.hasOwnProperty.call(input, key)) {
        const element = input[key];
        const newKey = this.aliasFields[key] || key;
        output[this.aliasFields[key] || key] = element;
        if (this.strict) {
          if (!this.attributes[newKey]) {
            delete output[newKey];
          }
        }
      }
    }
    return input;
  }

  constructor(initDefault?: IDefault) {
    super(initDefault);
  }
  init(initDefault?: IDefault): void {
    // console.log('init:', initDefault);

    super.init(initDefault);
  }
}
