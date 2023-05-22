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

  private getNodes(info: Info) {
    const nodes: string[] = [];
    this.host = Array.isArray(this.host)
      ? this.host
      : Array.isArray(info.host)
      ? info.host
      : this.host;
    this.port = Array.isArray(this.port)
      ? this.port
      : Array.isArray(info.port)
      ? info.port
      : this.port;
    if (Array.isArray(this.host) || Array.isArray(this.port)) {
      const hostA = Array.isArray(this.host) ? this.host : [this.host];

      const portA = Array.isArray(this.port) ? this.port : [this.port];

      const length = Math.max(hostA.length || 0, portA.length || 0);

      for (let index = 0; index < length; index++) {
        const host = hostA[index] || hostA[hostA.length - 1];
        const port = portA[index] || portA[portA.length - 1];

        nodes.push(
          (!host.includes('://')
            ? (info.connectionType || 'https') + '://'
            : '') +
            host +
            ':' +
            (port || 9200)
        );
      }
    } else {
      nodes.push(
        (!this.host.includes('://')
          ? (info.connectionType || 'https') + '://'
          : '') +
          this.host +
          ':' +
          (this.port || 9200)
      );
    }
    return nodes;
  }

  constructor(
    info: Info,
    journaly: SenderReceiver<any>,
    elasticOptions?: ClientOptions6 | ClientOptions7 | ClientOptions8
  ) {
    super(info, journaly);
    const nodes = this.getNodes(info);
    const object = {};
    if (nodes.length > 1) {
      object['nodes'] = nodes;
    } else {
      object['node'] = nodes[0];
    }
    this.elasticOptions = {
      ...object,
      ...{
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
