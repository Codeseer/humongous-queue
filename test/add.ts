import { MongoQueue } from "../mongo-queue";
import {MongoClient, Db} from "mongodb"
import { assert } from "chai";
import { resolve } from "url";

const url = 'mongodb://localhost:27017';

const sleep = (time: number) => {
  return new Promise((res) => {
    setTimeout(res, time)
  })
}

describe('multi get', () => {
  let db: Db;
  beforeEach(async function() {
    let client = await MongoClient.connect(url)
    db = client.db('queue-test')
  })
  afterEach(async function() {
    await db.dropDatabase()
  })
  it('add multiple payloads that can be gotten', async () => {
    let collection = db.collection('queue-test')
    let queue = new MongoQueue(collection, {delay: 0, visibility: 30})
    let result = await queue.add([{test: 'test0'}, {test: 'test1'}, {test: 'test2'}])
    console.log(result)
    let results = await queue.get(3)
    console.log(results)
    assert(results.length)
  })
  it('add multiple payloads that can be gotten after delay', async function() {
    this.timeout(10000)
    let collection = db.collection('queue-test')
    let queue = new MongoQueue(collection, {delay: 3, visibility: 30})
    let result = await queue.add([{test: 'test0'}, {test: 'test1'}, {test: 'test2'}])
    console.log(result)
    let results1 = await queue.get(3)
    assert(results1.length == 0)
    await sleep(3000)
    let results2 = await queue.get(3)
    assert(results2.length)
  })
})