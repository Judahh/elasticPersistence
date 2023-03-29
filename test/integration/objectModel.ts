import BaseModelDefault from '../../source/baseModelDefault';

export default class ObjectModel extends BaseModelDefault {
  generateName(): void {
    this.setName('Object');
  }
  protected attributes = {
    // Model attributes are defined here
    id: 'number',
    test: 'string',
    testNumber: 'decimal',
  };

  protected options = {
    timestamps: false,
  };
}
