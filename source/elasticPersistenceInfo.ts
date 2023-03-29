/* eslint-disable @typescript-eslint/no-explicit-any */
// file deepcode ignore no-any: any needed
import { Info, PersistenceInfo } from 'flexiblepersistence';
import { SenderReceiver } from 'journaly';
import { ClientOptions as ClientOptions6 } from 'es6';
import { ClientOptions as ClientOptions7 } from 'es7';
import { ClientOptions as ClientOptions8 } from 'es8';

// const version = (process.env.ES_VERSION || '8').split('.')[0].trim();

// const ClientOptions =
//   version === '6'
//     ? ClientOptions6
//     : version === '7'
//     ? ClientOptions7
//     : ClientOptions8;

export class ElasticPersistenceInfo extends PersistenceInfo {
  elasticOptions?: ClientOptions6 | ClientOptions7 | ClientOptions8;

  constructor(
    info: Info,
    journaly: SenderReceiver<any>,
    elasticOptions?: ClientOptions6 | ClientOptions7 | ClientOptions8
  ) {
    super(info, journaly);
    this.elasticOptions = {
      ...{
        node:
          (info.connectionType || 'https') +
          '://' +
          info.host +
          ':' +
          (info.port || 9200),
        auth: {
          username: info.username,
          password: info.password,
        },
        ssl: info.ssl,
        version: process.env.ES_VERSION,
      },
      ...(elasticOptions || {}),
    } as ClientOptions6 | ClientOptions7 | ClientOptions8;
    console.log(this.elasticOptions);
  }
}
