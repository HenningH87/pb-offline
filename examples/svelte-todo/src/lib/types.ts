import type { PBRecord } from 'pb-offline';

export interface Todo extends PBRecord {
  text: string;
  done: boolean;
  owner: string;
}
