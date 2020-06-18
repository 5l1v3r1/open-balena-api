import _ = require('lodash');
import { expect } from './test-lib/chai';
import { LokiBackend } from '../src/lib/device-logs/backends/loki';

const sleep = (milliseconds: number) =>
	new Promise((resolve) => setTimeout(resolve, milliseconds));

const createLog = (extra = {}) => {
	return {
		isStdErr: true,
		isSystem: true,
		message: `a log line`,
		timestamp: Date.now(),
		createdAt: Date.now(),
		...extra,
	};
};

const createContext = (extra = {}) => {
	return {
		id: 1,
		uuid: '1',
		belongs_to__application: 1,
		images: [],
		...extra,
	};
};

describe('loki backend', () => {
	it('should successfully publish log', async () => {
		const loki = new LokiBackend();
		const response = await loki.publish(createContext(), [createLog()]);
		expect(response).to.be.not.null;
	});

	it('should store and retrieve device log', async () => {
		const loki = new LokiBackend();
		const ctx = createContext();
		const log = createLog();
		const response = await loki.publish(ctx, [_.clone(log)]);
		expect(response).to.be.not.null;
		const history = await loki.history(ctx, 1000);
		expect(history[0]).to.deep.equal(log);
	});

	it('should convert multiple logs with different labels to streams and then back to logs', function () {
		const loki = new LokiBackend();
		const ctx = createContext();
		const logs = [
			createLog(),
			createLog(),
			createLog({ isStdErr: false }),
			createLog({ isStdErr: false }),
			createLog({ isStdErr: false, isSystem: false }),
		];
		// @ts-ignore usage of private function
		const streams = loki.fromDeviceLogsToStreams(ctx, _.cloneDeep(logs));
		expect(streams.length).to.be.equal(
			3,
			'should be 3 streams since 5 logs have 3 distinct sets of labels',
		);
		// @ts-ignore usage of private function
		const logsFromStreams = loki.fromStreamsToDeviceLogs(streams);
		expect(logsFromStreams).to.deep.equal(logs);
	});

	it('should push multiple logs with different labels and return in order', async function () {
		const loki = new LokiBackend();
		const ctx = createContext();
		const logs = [
			createLog({ timestamp: Date.now() - 4 }),
			createLog({ timestamp: Date.now() - 3 }),
			createLog({ timestamp: Date.now() - 2, isStdErr: false }),
			createLog({ timestamp: Date.now() - 1, isStdErr: false }),
			createLog({ timestamp: Date.now(), isStdErr: false, isSystem: false }),
		];
		const response = await loki.publish(ctx, _.cloneDeep(logs));
		expect(response).to.be.not.null;
		const history = await loki.history(ctx, 1000);
		expect(history.slice(0, 5)).to.deep.equal(
			_.orderBy(logs, 'timestamp', 'desc'),
		);
	});

	it('should de-duplicate multiple identical logs', async function () {
		const loki = new LokiBackend();
		const ctx = createContext();
		const log = createLog();
		const logs = [_.clone(log), _.clone(log), _.clone(log)];
		const response = await loki.publish(ctx, _.cloneDeep(logs));
		expect(response).to.be.not.null;
		const history = await loki.history(ctx, 1000);
		expect(history[1].timestamp).to.not.equal(log.timestamp);
	});

	it('should subscribe and receive a published logs', async function () {
		return new Promise(async (resolve, reject) => {
			const ctx = createContext();
			const loki = new LokiBackend();
			let received = false;
			loki.subscribe(ctx, (incomingLog) => {
				received = true;
				try {
					expect(incomingLog).to.deep.equal(log);
					resolve();
				} catch (err) {
					reject(err);
				}
			});
			const log = createLog();
			loki.publish(ctx, [_.clone(log)]);
			await sleep(1500);
			if (!received) {
				reject(Error('Subscription did not receive log'));
			}
		});
	});
});
