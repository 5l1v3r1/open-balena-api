import * as _ from 'lodash';
import * as grpc from 'grpc';
import { EventEmitter } from 'events';

import {
	createInsecureCredentials,
	createOrgIdMetadata,
	Direction,
	EntryAdapter,
	PusherClient,
	PushRequest,
	PushResponse,
	QuerierClient,
	QueryRequest,
	QueryResponse,
	StreamAdapter,
	TailRequest,
	TailResponse,
	Timestamp,
} from 'loki-grpc-client';
import { sbvrUtils } from '@resin/pinejs';
import { LOKI_HOST, LOKI_PORT } from '../../../lib/config';
import {
	DeviceLog,
	DeviceLogsBackend,
	LogContext,
	LogWriteContext,
	Subscription,
} from '../struct';
import { captureException } from '../../../platform/errors';

const { BadRequestError } = sbvrUtils;

const MAX_RETRIES = 10;
const MIN_BACKOFF = 100;
const MAX_BACKOFF = 10 * 1000;
const VERSION = 1;

function sleep(ms: number) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function createTimestampFromDate(date = new Date()) {
	const timestamp = new Timestamp();
	timestamp.fromDate(date);
	return timestamp;
}

function createTimestampFromMilliseconds(milliseconds: number) {
	const MS_TO_NANOS = 1000000;
	const seconds = Math.floor(milliseconds / 1000);
	const nanos = (milliseconds - seconds * 1000) * MS_TO_NANOS;

	const timestamp = new Timestamp();
	timestamp.setSeconds(seconds);
	timestamp.setNanos(nanos);
	return timestamp;
}

export class LokiBackend implements DeviceLogsBackend {
	private subscriptions: EventEmitter;
	private querier: QuerierClient;
	private pusher: PusherClient;
	private ready: boolean;
	private tailCalls: Map<string, grpc.ClientReadableStream<TailResponse>>;

	constructor() {
		this.subscriptions = new EventEmitter();
		this.querier = new QuerierClient(
			`${LOKI_HOST}:${LOKI_PORT}`,
			createInsecureCredentials(),
		);
		this.pusher = new PusherClient(
			`${LOKI_HOST}:${LOKI_PORT}`,
			createInsecureCredentials(),
		);
		this.ready = false;
		this.tailCalls = new Map();
	}

	public get available(): boolean {
		return !this.ready;
	}

	/**
	 *
	 * Return $count of logs matching device_id in descending (BACKWARD) order.
	 *
	 * The logs are sorted by timestamp since Loki returns a distinct stream for each label combination.
	 *
	 * @param ctx
	 * @param count
	 */
	public history(ctx: LogContext, count: number): Promise<DeviceLog[]> {
		return new Promise((resolve, reject) => {
			const oneHourAgo = new Date(Date.now() - 10000 * 60);

			const queryRequest = new QueryRequest();
			queryRequest.setSelector(this.getDeviceQuery(ctx));
			queryRequest.setLimit(Number.isFinite(count) ? count : 1000);
			queryRequest.setStart(createTimestampFromDate(oneHourAgo));
			queryRequest.setEnd(createTimestampFromDate());
			queryRequest.setDirection(Direction.BACKWARD);

			const deviceLogs: DeviceLog[] = [];
			const call = this.querier.query(
				queryRequest,
				createOrgIdMetadata(String(ctx.belongs_to__application)),
			);
			call.on('data', (queryResponse: QueryResponse) => {
				deviceLogs.push(
					...this.fromStreamsToDeviceLogs(queryResponse.getStreamsList()),
				);
			});
			call.on('error', (error: Error & { details: string }) => {
				reject(new BadRequestError(`Query failed: ${error.details}"`));
			});
			call.on('end', () => {
				resolve(_.orderBy(deviceLogs, 'timestamp', 'desc'));
			});
		});
	}

	public async publish(
		ctx: LogWriteContext,
		logs: Array<DeviceLog & { version?: number }>,
	): Promise<any> {
		const streams = this.fromDeviceLogsToStreams(ctx, logs);
		try {
			let tries = 0;
			let nextBackoff = MIN_BACKOFF;
			let prevBackoff = MIN_BACKOFF;
			while (nextBackoff <= MAX_BACKOFF) {
				try {
					tries += 1;
					return await this.push(ctx.belongs_to__application, streams);
				} catch (error) {
					// only retry when Loki is down or rate limiting
					if (
						tries <= MAX_RETRIES &&
						[grpc.status.UNAVAILABLE, grpc.status.RESOURCE_EXHAUSTED].includes(
							error.code,
						)
					) {
						this.ready = false;
						await sleep(nextBackoff);
						// fibonacci backoff
						nextBackoff = nextBackoff + prevBackoff;
						prevBackoff = nextBackoff - prevBackoff;
					} else {
						throw new BadRequestError(`Publish failed: ${error.details}`);
					}
				}
			}
		} finally {
			this.ready = true;
		}
	}

	private push(appId: number, streams: StreamAdapter[]): Promise<any> {
		return new Promise((resolve, reject) => {
			const pushRequest = new PushRequest();
			pushRequest.setStreamsList(streams);
			this.pusher.push(
				pushRequest,
				createOrgIdMetadata(String(appId)),
				(err: Error, pushResponse: PushResponse) => {
					if (err) {
						reject(err);
					} else {
						resolve(pushResponse);
					}
				},
			);
		});
	}

	public subscribe(ctx: LogContext, subscription: Subscription) {
		const key = this.getKey(ctx);
		if (!this.tailCalls.has(key)) {
			const request = new TailRequest();
			request.setQuery(this.getDeviceQuery(ctx));
			request.setStart(createTimestampFromMilliseconds(Date.now()));

			const call = this.querier.tail(
				request,
				createOrgIdMetadata(String(ctx.belongs_to__application)),
			);
			call.on('data', (response: TailResponse) => {
				const stream = response.getStream();
				if (stream) {
					const logs = this.fromStreamToDeviceLogs(stream);
					for (const log of logs) {
						this.subscriptions.emit(key, log);
					}
				}
			});
			call.on('error', (err: Error & { details: string }) => {
				if (err.details !== 'Cancelled') {
					captureException(err, 'Loki tail call error.');
				}
				this.subscriptions.removeListener(key, subscription);
				this.tailCalls.delete(key);
			});
			call.on('end', () => {
				this.subscriptions.removeListener(key, subscription);
				this.tailCalls.delete(key);
			});
			this.tailCalls.set(key, call);
		}
		this.subscriptions.on(key, subscription);
	}

	public unsubscribe(ctx: LogContext) {
		const key = this.getKey(ctx);
		const call = this.tailCalls.get(key);
		call?.cancel();
	}

	private getDeviceQuery(ctx: LogContext) {
		return `{device_id="${ctx.id}"}`;
	}

	private getKey(ctx: LogContext, suffix = 'logs') {
		return `app:${ctx.belongs_to__application}:device:${ctx.id}:${suffix}`;
	}

	private getLabels(ctx: LogContext, log: DeviceLog): string {
		return `{device_id="${ctx.id}", service_id="${
			log.serviceId ? log.serviceId : 'null'
		}", is_system="${log.isSystem}", is_std_err="${log.isStdErr}"}`;
	}

	private validateLog(log: DeviceLog) {
		if (typeof log.message !== 'string') {
			throw new BadRequestError('DeviceLog message must be string');
		} else if (typeof log.timestamp !== 'number') {
			throw new BadRequestError('DeviceLog timestamp must be number');
		} else if (typeof log.isSystem !== 'boolean') {
			throw new BadRequestError('DeviceLog isSystem must be boolean');
		} else if (typeof log.isStdErr !== 'boolean') {
			throw new BadRequestError('DeviceLog isStdErr must be boolean');
		} else if (
			typeof log.serviceId !== 'number' &&
			log.serviceId !== undefined
		) {
			throw new BadRequestError(
				'DeviceLog serviceId must be number or undefined',
			);
		}
	}

	private fromStreamsToDeviceLogs(streams: StreamAdapter[]): DeviceLog[] {
		return streams.flatMap(this.fromStreamToDeviceLogs);
	}

	private fromStreamToDeviceLogs(stream: StreamAdapter): DeviceLog[] {
		try {
			return stream.getEntriesList().map((entry: EntryAdapter) => {
				const log = JSON.parse(entry.getLine());
				if (log.version !== VERSION) {
					throw new Error(
						`Invalid Loki serialization version: ${JSON.stringify(log)}`,
					);
				}
				delete log.version;
				return log as DeviceLog;
			});
		} catch (err) {
			captureException(err, `Failed to convert stream to device log`);
			return [];
		}
	}

	private fromDeviceLogsToStreams(
		ctx: LogWriteContext,
		logs: Array<DeviceLog & { version?: number }>,
	) {
		const streams: StreamAdapter[] = [];
		const streamIndex: { [key: string]: StreamAdapter } = {}; // index streams by labels for fast lookup
		for (const log of logs) {
			this.validateLog(log);
			log.version = VERSION;
			const logJson = JSON.stringify(log);
			const entry = new EntryAdapter()
				.setLine(logJson)
				.setTimestamp(createTimestampFromMilliseconds(log.timestamp));
			const labels = this.getLabels(ctx, log);
			// append entry to stream
			let stream = streamIndex[labels];
			if (!stream) {
				// new stream if none exist for labels
				stream = new StreamAdapter();
				stream.setLabels(labels);
				streams.push(stream);
				streamIndex[labels] = stream;
			}
			stream.addEntries(entry);
		}
		return streams;
	}
}
