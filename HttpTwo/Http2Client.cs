using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using HttpTwo.Internal;
using System.Collections;

namespace HttpTwo
{
    public class Http2Client
    {
        Http2Connection connection;
        readonly IStreamManager streamManager;
        IFlowControlManager flowControlManager;

        public Http2Client(Uri uri, X509CertificateCollection certificates = null, IStreamManager streamManager = null, IFlowControlManager flowControlManager = null)
            : this(new Http2ConnectionSettings(uri, certificates), streamManager, flowControlManager)
        {
        }

        public Http2Client(string url, X509CertificateCollection certificates = null, IStreamManager streamManager = null, IFlowControlManager flowControlManager = null)
            : this(new Http2ConnectionSettings(url, certificates), streamManager, flowControlManager)
        {
        }

        public Http2Client(Http2ConnectionSettings connectionSettings, IStreamManager streamManager = null, IFlowControlManager flowControlManager = null)
        {
            this.flowControlManager = flowControlManager ?? new FlowControlManager();
            this.streamManager = streamManager ?? new StreamManager(this.flowControlManager);
            this.ConnectionSettings = connectionSettings;
        }

        public Http2ConnectionSettings ConnectionSettings { get; private set; }

        public IStreamManager StreamManager { get { return streamManager; } }
        public IFlowControlManager FlowControlManager { get { return flowControlManager; } }

        public async Task Connect()
        {
            if (connection != null && !connection.IsConnected())
            {
                connection.Disconnect();
                connection = null;
            }

            if (connection == null)
            {
                streamManager.Reset();
                connection = new Http2Connection(ConnectionSettings, this.streamManager, this.flowControlManager);
                await connection.Connect().ConfigureAwait(false);
            }

        }

        public async Task<Http2Response> Post(Uri uri, NameValueCollection headers = null, byte[] data = null)
        {
            return await Send(uri, HttpMethod.Post, headers, data).ConfigureAwait(false);
        }

        public async Task<Http2Response> Post(Uri uri, NameValueCollection headers = null, Stream data = null)
        {
            return await Send(uri, HttpMethod.Post, headers, data).ConfigureAwait(false);
        }

        public async Task<Http2Response> Send(Uri uri, HttpMethod method, NameValueCollection headers = null, byte[] data = null)
        {
            MemoryStream ms = null;

            if (data != null)
                ms = new MemoryStream(data);

            return await Send(new CancellationToken(), uri, method, headers, ms).ConfigureAwait(false);
        }

        public async Task<Http2Response> Send(Uri uri, HttpMethod method, NameValueCollection headers = null, Stream data = null)
        {
            return await Send(new CancellationToken(), uri, method, headers, data).ConfigureAwait(false);
        }

        public async Task<Http2Response> Send(
            CancellationToken cancelToken,
            Uri uri,
            HttpMethod method,
            NameValueCollection headers = null,
            Stream data = null)
        {
            var semaphoreClose = new SemaphoreSlim(0);

            await Connect();

            List<IFrame> frames = new List<IFrame>();
            var stream = await streamManager.Get().ConfigureAwait(false);
            stream.OnFrameReceived += (frame) =>
            {
                frames.Add(frame);
                // Check for an end of stream state
                if (stream.State == StreamState.HalfClosedRemote || stream.State == StreamState.Closed)
                {
                    semaphoreClose.Release();
                }

            };

            var sentEndOfStream = false;
            sentEndOfStream =
                await QueueHeaders(
                    sentEndOfStream,
                    method,
                    headers,
                    uri,
                    data,
                    stream);

            sentEndOfStream =
                await QueueData(
                    sentEndOfStream,
                    data,
                    stream);

            // Send an empty frame with end of stream flag
            if (!sentEndOfStream)
                await connection.QueueFrame(new DataFrame(stream.StreamIdentifer) { EndStream = true }).ConfigureAwait(false);

            if (!await semaphoreClose.WaitAsync(ConnectionSettings.ConnectionTimeout, cancelToken).ConfigureAwait(false))
                throw new TimeoutException();

            var responseData = new List<byte>();
            var rxHeaderData = new List<byte>();

            foreach (var f in frames)
            {
                if (f.Type == FrameType.Headers || f.Type == FrameType.Continuation)
                {
                    // Get the header data and add it to our buffer
                    var fch = (IFrameContainsHeaders)f;
                    if (fch.HeaderBlockFragment != null && fch.HeaderBlockFragment.Length > 0)
                        rxHeaderData.AddRange(fch.HeaderBlockFragment);
                }
                else if (f.Type == FrameType.PushPromise)
                {
                    // TODO: In the future we need to implement PushPromise beyond grabbing header data
                    var fch = (IFrameContainsHeaders)f;
                    if (fch.HeaderBlockFragment != null && fch.HeaderBlockFragment.Length > 0)
                        rxHeaderData.AddRange(fch.HeaderBlockFragment);
                }
                else if (f.Type == FrameType.Data)
                {
                    responseData.AddRange((f as DataFrame).Data);
                }
                else if (f.Type == FrameType.GoAway)
                {
                    var fga = f as GoAwayFrame;
                    if (fga != null && fga.AdditionalDebugData != null && fga.AdditionalDebugData.Length > 0)
                        responseData.AddRange(fga.AdditionalDebugData);
                }
            }

            var responseHeaders = Util.UnpackHeaders(connection.Decoder, rxHeaderData.ToArray());

            var strStatus = "500";
            if (responseHeaders[":status"] != null)
                strStatus = responseHeaders[":status"];

            var statusCode = HttpStatusCode.OK;
            Enum.TryParse<HttpStatusCode>(strStatus, out statusCode);

            // Remove the stream from being tracked since we're done with it
            await streamManager.Cleanup(stream.StreamIdentifer).ConfigureAwait(false);

            // Send a WINDOW_UPDATE frame to release our stream's data count
            // TODO: Eventually need to do this on the stream itself too (if it's open)
            await connection.FreeUpWindowSpace().ConfigureAwait(false);

            return new Http2Response
            {
                Status = statusCode,
                Stream = stream,
                Headers = responseHeaders,
                Body = responseData.ToArray()
            };
        }

        /// <summary>
        /// When sending multiple streams to one device, consecutive and immediate notifications need to be limited to 128.
        /// Notifications after 128 do not get delivered to the same device.
        /// </summary>
        /// <param name="cancelToken"></param>
        /// <param name="method"></param>
        /// <param name="headers"></param>
        /// <param name="requests"></param>
        /// <returns></returns>
        public async Task<Http2MultiResponse> SendMultipleAsync(
            CancellationToken cancelToken,
            HttpMethod method,
            NameValueCollection headers,
            List<Http2Request> requests)
        {
            var semaphoreClose = new SemaphoreSlim(0);

            await Connect();

            var frames = new List<IFrame>();
            var http2Streams = new List<Http2Stream>();
            var response = new Http2MultiResponse();

            foreach (var request in requests)
            {
                var sentEndOfStream = false;

                var apnsId = !string.IsNullOrWhiteSpace(request.ApnsId)
                    ? request.ApnsId
                    : Guid.NewGuid().ToString();

                var http2Stream = await streamManager.Get().ConfigureAwait(false);
                http2Streams.Add(http2Stream);

                // Initialize response maps
                response.Responses.Add(
                    new ApnsResponse
                    {
                        ApnsId = apnsId,
                        Stream = http2Stream,
                        Uri = request.Uri
                    });

                http2Stream.OnFrameReceived += (frame) =>
                {
                    frames.Add(frame);

                    // Check for an end of stream state
                    // Data frame with reason is returned when there is any error,
                    // so need to get only the Headers frames
                    if (frames.Where(f => f.Type == FrameType.Headers).Count() == requests.Count &&
                        http2Streams.All(stream =>
                            stream.State == StreamState.HalfClosedRemote ||
                            stream.State == StreamState.Closed))
                    {
                        semaphoreClose.Release();
                    }
                };

                sentEndOfStream =
                    await QueueHeaders(
                        sentEndOfStream,
                        method,
                        headers,
                        request.Uri,
                        request.DataStream,
                        http2Stream,
                        apnsId);

                sentEndOfStream =
                    await QueueData(
                        sentEndOfStream,
                        request.DataStream,
                        http2Stream);

                // Send an empty frame with end of stream flag
                if (!sentEndOfStream)
                    await connection
                        .QueueFrame(
                            new DataFrame(http2Stream.StreamIdentifer)
                            {
                                EndStream = true
                            })
                        .ConfigureAwait(false);
            }

            if (!await semaphoreClose.WaitAsync(
                        ConnectionSettings.ConnectionTimeout,
                        cancelToken)
                    .ConfigureAwait(false))
            {
                throw new TimeoutException();
            }

            var responseData = new List<byte>();
            var rxHeaderData = new List<byte>();

            foreach (var f in frames)
            {
                if (f.Type == FrameType.Headers || f.Type == FrameType.Continuation)
                {
                    // Get the header data and add it to our buffer
                    var fch = (IFrameContainsHeaders)f;
                    if (fch.HeaderBlockFragment != null && fch.HeaderBlockFragment.Length > 0)
                        rxHeaderData.AddRange(fch.HeaderBlockFragment);
                }
                else if (f.Type == FrameType.PushPromise)
                {
                    // TODO: In the future we need to implement PushPromise beyond grabbing header data
                    var fch = (IFrameContainsHeaders)f;
                    if (fch.HeaderBlockFragment != null && fch.HeaderBlockFragment.Length > 0)
                        rxHeaderData.AddRange(fch.HeaderBlockFragment);
                }
                else if (f.Type == FrameType.Data)
                {
                    responseData.AddRange((f as DataFrame).Data);
                }
                else if (f.Type == FrameType.GoAway)
                {
                    var fga = f as GoAwayFrame;
                    if (fga != null && fga.AdditionalDebugData != null && fga.AdditionalDebugData.Length > 0)
                        responseData.AddRange(fga.AdditionalDebugData);
                }
            }

            var responseHeaders = Util.UnpackHeaders(connection.Decoder, rxHeaderData.ToArray());

            MapResponses(responseHeaders, response.Responses);
            await CleanUp(http2Streams);

            response.Headers = responseHeaders;
            response.Body = responseData.ToArray();

            return response;
        }

        private void MapResponses(
            NameValueCollection responseHeaders,
            List<ApnsResponse> responses)
        {
            var apnsIds =
                responseHeaders["apns-id"] != null
                    ? responseHeaders["apns-id"].Split(',').ToList()
                    : Enumerable.Empty<string>().ToList();

            var statuses =
                responseHeaders[":status"] != null
                    ? responseHeaders[":status"].Split(',').ToList()
                    : Enumerable.Empty<string>().ToList();

            for (var idx = 0; idx < apnsIds.Count; idx++)
            {
                var statusCode = HttpStatusCode.Accepted;
                Enum.TryParse(statuses[idx], out statusCode);

                responses[idx].Status = statusCode;
            }
        }

        private async Task CleanUp(List<Http2Stream> http2Streams)
        {
            // Remove the stream from being tracked since we're done with it
            foreach (var stream in http2Streams)
            {
                await streamManager.Cleanup(stream.StreamIdentifer).ConfigureAwait(false);
            }

            // Send a WINDOW_UPDATE frame to release our stream's data count
            // TODO: Eventually need to do this on the stream itself too (if it's open)
            await connection.FreeUpWindowSpace().ConfigureAwait(false);
        }

        private async Task<bool> QueueHeaders(
            bool sentEndOfStream,
            HttpMethod method,
            NameValueCollection headers,
            Uri uri,
            Stream dataStream,
            Http2Stream http2Stream,
            string apnsId = null)
        {
            var allHeaders = new NameValueCollection
            {
                { ":method", method.Method.ToUpperInvariant() },
                { ":path", uri.PathAndQuery },
                { ":scheme", uri.Scheme },
                { ":authority", uri.Authority }
            };

            if (headers != null && headers.Count > 0)
                allHeaders.Add(headers);

            if (apnsId != null)
            {
                allHeaders.Add("apns-id", apnsId);
            }

            var headerData = Util.PackHeaders(connection.Encoder, allHeaders);

            var numFrames = (int)Math.Ceiling((double)headerData.Length / (double)connection.Settings.MaxFrameSize);

            for (int i = 0; i < numFrames; i++)
            {
                // First item is headers frame, others are continuation
                IFrameContainsHeaders frame = (i == 0) ?
                    (IFrameContainsHeaders)new HeadersFrame(http2Stream.StreamIdentifer)
                    : (IFrameContainsHeaders)new ContinuationFrame(http2Stream.StreamIdentifer);

                // Set end flag if this is the last item
                if (i == numFrames - 1)
                    frame.EndHeaders = true;

                var maxFrameSize = connection.Settings.MaxFrameSize;

                var amt = maxFrameSize;
                if (i * maxFrameSize + amt > headerData.Length)
                    amt = (uint)headerData.Length - (uint)(i * maxFrameSize);
                frame.HeaderBlockFragment = new byte[amt];
                Array.Copy(headerData, i * maxFrameSize, frame.HeaderBlockFragment, 0, amt);

                // If we won't send 
                if (dataStream == null && frame is HeadersFrame)
                {
                    sentEndOfStream = true;
                    (frame as HeadersFrame).EndStream = true;
                }

                await connection.QueueFrame(frame).ConfigureAwait(false);
            }

            return sentEndOfStream;
        }

        private async Task<bool> QueueData(bool sentEndOfStream, Stream data, Http2Stream stream)
        {
            if (data != null)
            {
                var supportsPosLength = true; // Keep track of if we threw exceptions trying pos/len of stream

                // Break stream up into data frames within allowed size
                var dataFrameBuffer = new byte[connection.Settings.MaxFrameSize];
                while (true)
                {

                    var rd = await data.ReadAsync(dataFrameBuffer, 0, dataFrameBuffer.Length).ConfigureAwait(false);

                    if (rd <= 0)
                        break;

                    // Make a new data frame with a buffer the size we read
                    var dataFrame = new DataFrame(stream.StreamIdentifer);
                    dataFrame.Data = new byte[rd];
                    // Copy over the data we read
                    Array.Copy(dataFrameBuffer, 0, dataFrame.Data, 0, rd);

                    try
                    {
                        // See if the stream supports Length / Position to try and detect EOS
                        // we also want to see if we previously had an exception trying this
                        // and not try again if we did, since throwing exceptions every single
                        // read operation is wasteful
                        if (supportsPosLength && data.Position >= data.Length)
                        {
                            dataFrame.EndStream = true;
                            sentEndOfStream = true;
                        }
                    }
                    catch
                    {
                        supportsPosLength = false;
                        sentEndOfStream = false;
                    }

                    await connection.QueueFrame(dataFrame).ConfigureAwait(false);
                }
            }

            return sentEndOfStream;
        }

        public async Task<bool> Ping(byte[] opaqueData, CancellationToken cancelToken)
        {
            if (opaqueData == null || opaqueData.Length <= 0)
                throw new ArgumentNullException("opaqueData");

            await Connect();

            var semaphoreWait = new SemaphoreSlim(0);
            var opaqueDataMatch = false;

            var connectionStream = await streamManager.Get(0).ConfigureAwait(false);

            Http2Stream.FrameReceivedDelegate frameRxAction;
            frameRxAction = new Http2Stream.FrameReceivedDelegate(frame =>
            {
                var pf = frame as PingFrame;
                if (pf != null)
                {
                    opaqueDataMatch = pf.Ack && pf.OpaqueData != null && pf.OpaqueData.SequenceEqual(opaqueData);
                    semaphoreWait.Release();
                }
            });

            // Wire up the event to listen for ping response
            connectionStream.OnFrameReceived += frameRxAction;

            // Construct ping request
            var pingFrame = new PingFrame();
            pingFrame.OpaqueData = new byte[opaqueData.Length];
            opaqueData.CopyTo(pingFrame.OpaqueData, 0);

            // Send ping
            await connection.QueueFrame(pingFrame).ConfigureAwait(false);

            // Wait for either a ping response or timeout
            await semaphoreWait.WaitAsync(cancelToken).ConfigureAwait(false);

            // Cleanup the event
            connectionStream.OnFrameReceived -= frameRxAction;

            return opaqueDataMatch;
        }

        public async Task<bool> Disconnect()
        {
            return await Disconnect(Timeout.InfiniteTimeSpan).ConfigureAwait(false);
        }

        public async Task<bool> Disconnect(TimeSpan timeout)
        {
            if (connection == null)
            {
                return true;
            }

            if (!connection.IsConnected())
            {
                // Force disconnection
                connection.Disconnect();
                return true;
            }

            var connectionStream = await streamManager.Get(0).ConfigureAwait(false);

            var semaphoreWait = new SemaphoreSlim(0);
            var cancelTokenSource = new CancellationTokenSource();
            var sentGoAway = false;

            var sentDelegate = new Http2Stream.FrameSentDelegate(frame =>
            {
                if (frame.Type == FrameType.GoAway)
                {
                    sentGoAway = true;
                    semaphoreWait.Release();
                }
            });

            connectionStream.OnFrameSent += sentDelegate;

            await connection.QueueFrame(new GoAwayFrame()).ConfigureAwait(false);

            if (timeout != Timeout.InfiniteTimeSpan)
                cancelTokenSource.CancelAfter(timeout);

            await semaphoreWait.WaitAsync(cancelTokenSource.Token).ConfigureAwait(false);

            connectionStream.OnFrameSent -= sentDelegate;

            connection.Disconnect();

            return sentGoAway;
        }

        public class Http2Response
        {
            public HttpStatusCode Status { get; set; }
            public Http2Stream Stream { get; set; }
            public NameValueCollection Headers { get; set; }
            public byte[] Body { get; set; }
        }

        public class Http2MultiResponse
        {
            public List<ApnsResponse> Responses { get; set; }
            public NameValueCollection Headers { get; set; }
            public byte[] Body { get; set; }

            public Http2MultiResponse()
            {
                this.Responses = new List<ApnsResponse>();
            }
        }

        public class ApnsResponse
        {
            public string ApnsId { get; set; }
            public HttpStatusCode Status { get; set; }
            public Http2Stream Stream { get; set; }
            public Uri Uri { get; set; }
        }

        public class Http2Request
        {
            public string ApnsId { get; set; }
            public Uri Uri { get; set; }
            public Stream DataStream { get; set; }
        }
    }
}
