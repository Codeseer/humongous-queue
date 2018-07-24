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
  let client: MongoClient
  beforeEach(async function() {
    client = await MongoClient.connect(url)
    db = client.db('queue-test')
  })
  afterEach(async function() {
    await db.dropDatabase()
    await client.close()
  })
  it('add multiple payloads that can be gotten', async () => {
    let collection = db.collection('queue-test')
    let queue = new MongoQueue(collection, {delay: 0, visibility: 30})
    let result = await queue.add([{test: 'test0'}, {test: 'test1'}, {test: 'test2'}])
    let results = await queue.get(3)
    assert(results.length)
  })
  it('add multiple payloads that can be gotten after delay', async function() {
    this.timeout(10000)
    let collection = db.collection('queue-test')
    let queue = new MongoQueue(collection, {delay: 3, visibility: 30})
    let result = await queue.add([{test: 'test0'}, {test: 'test1'}, {test: 'test2'}])
    let results1 = await queue.get(3)
    assert(results1.length == 0)
    await sleep(3000)
    let results2 = await queue.get(3)
    assert(results2.length)
  })
  it('does not get the same msgs', async function() {
    this.timeout(10000)
    let collection = db.collection('queue-test')
    let queue = new MongoQueue(collection, {delay: 0, visibility: 30})
    let result = await queue.add([{test: 'test0'}, {test: 'test1'}, {test: 'test2'}])
    let results1 = await queue.get(1)
    let results2 = await queue.get(1)
    let results3 = await queue.get(1)
    assert(results1[0].payload.test == 'test0')
    assert(results2[0].payload.test == 'test1')
    assert(results3[0].payload.test == 'test2')
  })
  it('does not schedule messages with the same key more than once.', async function () {
    this.timeout(10000)
    let collection = db.collection('queue-test')
    let queue = new MongoQueue(collection, {delay: 0, visibility: 30})
    let result = await queue.add([{key: 'test0', test: 'test0'}, {key: 'test0', test: 'test1'}, {key: 'test0', test: 'test2'}])
    let results1 = await queue.get(3)
    assert(results1.length == 1)
    assert(results1[0].payload.test == 'test0')
  })
  it('deletes from the queue when an message is ackd', async function () {
    this.timeout(10000)
    let collection = db.collection('queue-test')
    let queue = new MongoQueue(collection, {delay: 0, visibility: 30})
    let result = await queue.add([{test: 'test0'}, {test: 'test1'}, {test: 'test2'}])
    let results1 = await queue.get(1)
    await queue.ack(results1[0].ack)
    let results2 = await queue.get(3)
    assert(results2.length == 2)
  })
  it('shows up in queue after visibility timeout.', async function () {
    this.timeout(10000)
    let collection = db.collection('queue-test')
    let queue = new MongoQueue(collection, {delay: 0, visibility: 3})
    let result = await queue.add([{test: 'test0'}, {test: 'test1'}, {test: 'test2'}])
    let results1 = await queue.get(3)
    assert(results1.length == 3)
    let results2 = await queue.get(3)
    assert(results2.length == 0)
    await sleep(3000)
    let results3 = await queue.get(3)
    assert(results3.length == 3)
  })
  it('ping extends visibility timeout', async function () {
    this.timeout(10000)
    let collection = db.collection('queue-test')
    let queue = new MongoQueue(collection, {delay: 0, visibility: 3})
    let result = await queue.add([{test: 'test0'}, {test: 'test1'}, {test: 'test2'}])
    let results1 = await queue.get(3)
    assert(results1.length == 3)
    let results2 = await queue.get(3)
    assert(results2.length == 0)
    await sleep(2000)
    await queue.ping(results1[0].ack)
    await sleep(2000)
    let results3 = await queue.get(3)
    assert(results3.length == 2)
  })
  it('delay can be set as a Date', async function () {
    this.timeout(10000)
    let collection = db.collection('queue-test')
    let queue = new MongoQueue(collection, {delay: 0, visibility: 3})
    let result = await queue.add([{test: 'test0'}, {test: 'test1'}, {test: 'test2'}], new Date(Date.now() + 3000))
    let results1 = await queue.get(3)
    assert(results1.length == 0)
    await sleep(3000)
    let results2 = await queue.get(3)
    assert(results2.length == 3)
  })
})