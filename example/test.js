'use strict';

var kafka = require('..');
var Consumer = kafka.Consumer;
var Offset = kafka.Offset;
var Client = kafka.KafkaClient;
var argv = require('optimist').argv;
var topic = argv.topic || 'topic1';
var host = argv.host || 'localhost:9092';
var partition = argv.partition || '2';
var orgid = argv.orgid || '50';
var ip = argv.ip || '127.0.0.1';
var epid = argv.epid || '000000';

var _ = require('underscore');

//var client = new Client({ kafkaHost: 'localhost:9092' });

var client = new Client({ kafkaHost: host });
var topics = [];
//var topics = [{ topic: topic, partition: 1 }, { topic: topic, partition: 0 }];
_.each(_.range(parseInt(partition)), function(p) {
  topics.push({ topic: topic, partition: p });
});
console.log("Topics are " + JSON.stringify(topics));

var options = { autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024 * 20 };

var consumer = new Consumer(client, topics, options);
var offset = new Offset(client);

consumer.on('message', function (message) {
  // console.log(message);
  var key = message.key;
  var value = message.value;

  if ('cmdctr-real-time' == topic) {
    var k_params = key.split('_');
    if (k_params[0] == orgid && k_params[1] == epid) {
      console.log(key + ' -:- ' + value);
    }
    return;
  }

  var lines = message.value.split('\n');
  _.filter(lines, function(msg)  {
    var params = msg.split(',');
    //if (params[5] == orgid) {
    if (params[0] == ip && params[5] == orgid) {
      console.log(key + ' -:- ' + msg)
    }
  });
});

consumer.on('error', function (err) {
  console.log('error', err);
});

/*
* If consumer get `offsetOutOfRange` event, fetch data from the smallest(oldest) offset
*/
consumer.on('offsetOutOfRange', function (topic) {
  topic.maxNum = 2;
  offset.fetch([topic], function (err, offsets) {
    if (err) {
      return console.error(err);
    }
    console.log("Offsets : " + JSON.stringify(offsets));

    var max = Math.max.apply(null, offsets[topic.topic][topic.partition]);
    consumer.setOffset(topic.topic, topic.partition, max);
  });
});
