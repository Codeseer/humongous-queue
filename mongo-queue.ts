import { Collection } from "mongodb";
import uuidv4 from 'uuid/v4'
interface Payload {
  key: string;
}

interface QueueOptions {
  visibility: Number,
  delay: Number|Date,
  maxRetries: Number,
  deadQueue: Collection
}
class MongoQueue {
  private collection: Collection;
  private visibility: Number;
  private delay: Number | Date;
  private maxRetries: Number;
  private deadQueue: Collection;

  constructor(collection: Collection, opts: QueueOptions) {
    this.collection = collection
    this.visibility = opts.visibility
    this.delay = opts.delay
    this.maxRetries = opts.maxRetries
    this.deadQueue = opts.deadQueue
  }

  private nowPlusSecs(secs) {
    return (new Date(Date.now() + secs * 1000)).toISOString()
  }

  private getVisibilityDate() {
    if(this.delay instanceof Date) {
      return this.delay
    } else {
      return this.nowPlusSecs(this.delay)
    }
  }

  async get(quantity: number) {
    let checkout = uuidv4()
    let now = new Date()
    let updateOne = {
      updateOne: {
        filter: {
          visible: {
            $lte: now
          },
          deleted: null
        },
        update: {
          $inc : { tries : 1 },
          $set: {
            checkout,
            ack: uuidv4(),
            visible: this.nowPlusSecs(this.visibility)
          }
        }
      }
    }
    let writes = []
    for(let i = 0; i < quantity; i++) {
      writes.push(updateOne)
    }
    await this.collection.bulkWrite(writes)
    let msgs = await this.collection.find({checkout, deleted: null})

    return msgs
  }

  async add(payload: Payload|Array<Payload>) {
    let payloads = new Array<Payload>().concat(payload)
    let writes = []
    if (payloads.length === 0) {
        throw new Error('Queue.add(): Array payload length must be greater than 0')
    }
    for(let p of payloads) {
      let pKey = p.key || uuidv4()
      writes.push({
        updateOne: {
          filter: {
            key: pKey
          },
          update: {
            $setOnInsert: {
              visible  : this.getVisibilityDate(),
              payload  : p,
              key: pKey
            }
          },
          upsert: true
        }
      })
    }
    return await this.collection.bulkWrite(writes)
  }

  async ack(ack: string) {
    var query = {
        ack,
        visible : { $gt : new Date() },
        deleted : null
    }
    var update = {
        $set : {
            deleted : new Date(),
        }
    }
    return await this.collection.findOneAndUpdate(query, update, { returnOriginal : false })
  }

  async ping(ack) {
    var query = {
        ack     : ack,
        visible : { $gt : new Date() },
        deleted : null,
    }
    var update = {
        $set : {
            visible : this.nowPlusSecs(this.visibility)
        }
    }
    return await this.collection.findOneAndUpdate(query, update, { returnOriginal : false })
  }

  async total() {
    self.col.count(function(err, count) {
      if (err) return callback(err)
      callback(null, count)
    })
  }
}