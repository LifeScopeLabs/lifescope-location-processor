import AWS from 'aws-sdk';
import _ from 'lodash';
import config from 'config';
import moment from 'moment';
import mongodb from 'mongodb';

import uuid from './lib/util/uuid';


const sliceSize = 1000;

const AWSAccessKeyId = config.aws.credentials.AWSAccessKeyId;
const AWSSecretKey = config.aws.credentials.AWSSecretKey;


const AWSConfig = {
	"accessKeyId": AWSAccessKeyId,
	"secretAccessKey": AWSSecretKey,
	"region": 'us-east-1'
};

// Set aws config
AWS.config.update(AWSConfig);

let s3 = new AWS.S3({apiVersion: '2006-03-01'});

let mongo;


async function dequeue() {
	let nextFile;

	try {
		nextFile = await mongo.db('live').collection('location_files').findOne({
			status: 'ready'
		}, {
			sort: {
				queue_time: -1
			}
		});

		if (nextFile == null) {
			console.log('No files to run, pausing for 5 minutes');
			setTimeout(dequeue, 300000)
		}
		else {
			console.log('Running the next location file for user ' + nextFile.user_id.toString('hex'));

			await mongo.db('live').collection('location_files').updateOne({
				_id: nextFile._id
			}, {
				$set: {
					status: 'running'
				}
			});

			let file = await new Promise(function(resolve, reject) {
				s3.getObject({
					Bucket : config.aws.s3.locations.bucket_name,
					Key: nextFile.user_id.toString('hex') + '/' + moment(nextFile.upload_time).utc().unix() + '.json',
				}, async function(err, data) {
					if (err) {
						reject(err);
					}
					else {
						resolve(data);
					}
				});
			});

			let parsed = JSON.parse(file.Body);

			let locations = parsed.locations;


			console.log('Starting to parse locations');
			let counter = 0;
			let startIndex = 0;
			let parsedLocations = [];
			let finished = false;

			_.each(locations, async function(location) {
				if (location.latitudeE7 && location.longitudeE7 && location.timestampMs) {
					let datetime = moment(parseInt(location.timestampMs));

					let document = {
						identifier: 'uploaded:::' + nextFile.user_id.toString('hex') + ':::' + datetime,
						estimated: false,
						datetime: moment(datetime).utc().toDate(),
						geo_format: 'lat_lng',
						geolocation: [location.longitudeE7 / 10000000, location.latitudeE7 / 10000000],
						uploaded: true,
						updated: moment().utc().toDate(),
						user_id: nextFile.user_id
					};

					parsedLocations.push(document);
				}
			});

			while (!finished) {
				let bulkLocations = mongo.db('live').collection('locations').initializeUnorderedBulkOp();
				let slice = parsedLocations.slice(startIndex, startIndex + sliceSize);

				if (slice.length > 0) {
					_.each(slice, function(document) {
						bulkLocations.find({
							identifier: document.identifier,
							user_id: nextFile.user_id
						})
							.upsert()
							.updateOne({
								$set: document,
								$setOnInsert: {
									_id: uuid(uuid()),
									created: document.updated
								}
							});
					});

					try {
						await bulkLocations.execute();
					} catch(err) {
						console.log(err);
					}

					startIndex += sliceSize;
				}

				if (slice.length < sliceSize) {
					finished = true;
				}
			}

			console.log('Finished uploading locations');

			await new Promise(function(resolve, reject) {
				s3.deleteObject({
					Bucket : config.aws.s3.locations.bucket_name,
					Key: nextFile.user_id.toString('hex') + '/' + moment(nextFile.upload_time).utc().unix() + '.json',
				}, async function(err, data) {
					if (err) {
						reject(err);
					}
					else {
						resolve(data);
					}
				});
			});

			await mongo.db('live').collection('location_files').removeOne({
				_id: nextFile._id
			});

			dequeue();
		}
	} catch(err) {
		console.log('Error in location parsing');
		console.log(err);

		await mongo.db('live').collection('location_files').updateOne({
			_id: nextFile._id
		}, {
			$set: {
				status: 'ready',
				queue_time: moment(nextFile.queue_time).add(6, 'hours').toDate()
			}
		});

		dequeue();
	}
}



(async function() {
	mongo = await mongodb.MongoClient.connect(config.mongodb.address, config.mongodb.options);

	//Randomize the start time so that multiple threads don't all simultaneously grab the same job on boot.
	let waitTime = Math.random() * 10000;

	setTimeout(dequeue, waitTime);
})();