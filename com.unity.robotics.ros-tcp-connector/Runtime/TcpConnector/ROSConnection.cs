using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;
using UnityEngine;
using UnityEngine.Serialization;
using System.Collections.Concurrent;
using System.Threading;
using System.Linq;
using Stopwatch = System.Diagnostics.Stopwatch;

namespace Unity.Robotics.ROSTCPConnector
{
    public class ROSConnection : MonoBehaviour
    {
        public const string k_Version = "v0.7.0";
        public const string k_CompatibleVersionPrefix = "v0.7.";

        // Variables required for ROS communication
        [SerializeField]
        [FormerlySerializedAs("hostName")]
        [FormerlySerializedAs("rosIPAddress")]
        string m_RosIPAddress = "127.0.0.1";
        public string RosIPAddress { get => m_RosIPAddress; set => m_RosIPAddress = value; }

        [SerializeField]
        [FormerlySerializedAs("hostPort")]
        [FormerlySerializedAs("rosPort")]
        int m_RosPort = 10000;
        public int RosPort { get => m_RosPort; set => m_RosPort = value; }

        [SerializeField]
        bool m_ConnectOnStart = true;
        public bool ConnectOnStart { get => m_ConnectOnStart; set => m_ConnectOnStart = value; }

        [SerializeField]
        [Tooltip("If nothing has been sent for this long (seconds), send a keepalive message to check the connection is still working.")]
        float m_KeepaliveTime = 1;
        public float KeepaliveTime { get => m_KeepaliveTime; set => m_KeepaliveTime = value; }

        [SerializeField]
        float m_NetworkTimeoutSeconds = 2;
        public float NetworkTimeoutSeconds { get => m_NetworkTimeoutSeconds; set => m_NetworkTimeoutSeconds = value; }

        [SerializeField]
        float m_SleepTimeSeconds = 0.01f;
        public float SleepTimeSeconds { get => m_SleepTimeSeconds; set => m_SleepTimeSeconds = value; }


        [SerializeField]
        [FormerlySerializedAs("showHUD")]
        bool m_ShowHUD = true;
        public bool ShowHud { get => m_ShowHUD; set => m_ShowHUD = value; }

        [SerializeField]
        string[] m_TFTopics = { "/tf" };
        public string[] TFTopics { get => m_TFTopics; set => m_TFTopics = value; }

        const int k_DefaultPublisherQueueSize = 10;
        const bool k_DefaultPublisherLatch = false;

        // GUI window variables
        internal HudPanel m_HudPanel = null;
        public HudPanel HUDPanel => m_HudPanel;

        class OutgoingMessageQueue
        {
            ConcurrentQueue<OutgoingMessageSender> m_OutgoingMessageQueue;
            public readonly ManualResetEvent NewMessageReadyToSendEvent;

            public OutgoingMessageQueue()
            {
                m_OutgoingMessageQueue = new ConcurrentQueue<OutgoingMessageSender>();
                NewMessageReadyToSendEvent = new ManualResetEvent(false);
            }

            public void Enqueue(OutgoingMessageSender outgoingMessageSender)
            {
                m_OutgoingMessageQueue.Enqueue(outgoingMessageSender);
                NewMessageReadyToSendEvent.Set();
            }

            public bool TryDequeue(out OutgoingMessageSender outgoingMessageSender)
            {
                return m_OutgoingMessageQueue.TryDequeue(out outgoingMessageSender);
            }
        }

        OutgoingMessageQueue m_OutgoingMessageQueue = new OutgoingMessageQueue();

        ConcurrentQueue<Tuple<string, byte[]>> m_IncomingMessages = new ConcurrentQueue<Tuple<string, byte[]>>();
        CancellationTokenSource m_ConnectionThreadCancellation;
        public bool HasConnectionThread => m_ConnectionThreadCancellation != null;

        static bool m_HasConnectionError = false;
        public bool HasConnectionError => m_HasConnectionError;

        // only the main thread can access Time.*, so make a copy here
        public static float s_RealTimeSinceStartup = 0.0f;

        readonly object m_ServiceRequestLock = new object();

        int m_NextSrvID = 101;
        Dictionary<int, TaskPauser> m_ServicesWaiting = new Dictionary<int, TaskPauser>();

        public bool listenForTFMessages = true;

        float m_LastMessageReceivedRealtime;
        float m_LastMessageSentRealtime;
        public float LastMessageReceivedRealtime => m_LastMessageReceivedRealtime;
        public float LastMessageSentRealtime => m_LastMessageSentRealtime;

        // For the IP address field we show in the hud, we store the IP address and port in PlayerPrefs.
        // This is used to remember the last IP address the player typed into the HUD, in builds where ConnectOnStart is not checked
        public const string PlayerPrefsKey_ROS_IP = "ROS_IP";
        public const string PlayerPrefsKey_ROS_TCP_PORT = "ROS_TCP_PORT";
        public static string RosIPAddressPref => PlayerPrefs.GetString(PlayerPrefsKey_ROS_IP, "127.0.0.1");
        public static int RosPortPref => PlayerPrefs.GetInt(PlayerPrefsKey_ROS_TCP_PORT, 10000);

        public bool HasSubscriber(string topic)
        {
            RosTopicState info;
            return m_Topics.TryGetValue(topic, out info) && info.HasSubscriberCallback;
        }

        MessageSerializer m_MessageSerializer = new MessageSerializer();
        MessageDeserializer m_MessageDeserializer = new MessageDeserializer();
        List<Action<string[]>> m_TopicsListCallbacks = new List<Action<string[]>>();
        List<Action<Dictionary<string, string>>> m_TopicsAndTypesListCallbacks = new List<Action<Dictionary<string, string>>>();
        List<Action<RosTopicState>> m_NewTopicCallbacks = new List<Action<RosTopicState>>();

        Dictionary<string, RosTopicState> m_Topics = new Dictionary<string, RosTopicState>();

        public void ListenForTopics(Action<RosTopicState> callback, bool notifyAllExistingTopics = false)
        {
            m_NewTopicCallbacks.Add(callback);
            if (notifyAllExistingTopics)
            {
                foreach (RosTopicState state in AllTopics)
                {
                    callback(state);
                }
            }
        }

        RosTopicState AddTopic(string topic, string rosMessageName, bool isService = false)
        {
            RosTopicState newTopic = new RosTopicState(topic, rosMessageName, this, new InternalAPI(this), isService);
            lock (m_Topics)
            {
                m_Topics.Add(topic, newTopic);
            }
            return newTopic;
        }

        public RosTopicState GetTopic(string topic)
        {
            RosTopicState info;
            m_Topics.TryGetValue(topic, out info);
            return info;
        }

        public IEnumerable<RosTopicState> AllTopics => m_Topics.Values;

        public RosTopicState GetOrCreateTopic(string topic, string rosMessageName, bool isService = false)
        {
            RosTopicState state = GetTopic(topic);
            if (state != null)
            {
                if (state.RosMessageName != rosMessageName)
                {
                    state.ChangeRosMessageName(rosMessageName);
                }
                return state;
            }

            RosTopicState result = AddTopic(topic, rosMessageName, isService);
            foreach (Action<RosTopicState> callback in m_NewTopicCallbacks)
            {
                callback(result);
            }
            return result;
        }

        public void Subscribe<T>(string topic, Action<T> callback) where T : Message
        {
            string rosMessageName = MessageRegistry.GetRosMessageName<T>();
            AddSubscriberInternal(topic, rosMessageName, (Message msg) =>
            {
                if (msg.RosMessageName == rosMessageName)
                {
                    callback((T)msg);
                }
                else
                {
                    Debug.LogError($"Subscriber to '{topic}' expected '{rosMessageName}' but received '{msg.RosMessageName}'!?");
                }
            });
        }

        public void Unsubscribe(string topic)
        {
            RosTopicState info = GetTopic(topic);
            if (info != null)
                info.UnsubscribeAll();
        }

        // Version for when the message type is unknown at compile time
        public void SubscribeByMessageName(string topic, string rosMessageName, Action<Message> callback)
        {
            var constructor = MessageRegistry.GetDeserializeFunction(rosMessageName);
            if (constructor == null)
            {
                Debug.LogError($"Failed to subscribe to topic {topic} - no class has RosMessageName \"{rosMessageName}\"!");
                return;
            }

            AddSubscriberInternal(topic, rosMessageName, callback);
        }

        void AddSubscriberInternal(string topic, string rosMessageName, Action<Message> callback)
        {
            RosTopicState info;
            if (!m_Topics.TryGetValue(topic, out info))
            {
                info = AddTopic(topic, rosMessageName);
            }

            info.AddSubscriber(callback);

            foreach (Action<RosTopicState> topicCallback in m_NewTopicCallbacks)
            {
                topicCallback(info);
            }
        }

        // Implement a service in Unity
        public void ImplementService<TRequest, TResponse>(string topic, Func<TRequest, TResponse> callback, int? queueSize = null)
            where TRequest : Message
            where TResponse : Message
        {
            string rosMessageName = rosMessageName = MessageRegistry.GetRosMessageName<TRequest>();

            RosTopicState info;
            if (!m_Topics.TryGetValue(topic, out info))
            {
                info = AddTopic(topic, rosMessageName, isService: true);
            }

            int resolvedQueueSize = queueSize.GetValueOrDefault(k_DefaultPublisherQueueSize);
            info.ImplementService(callback, resolvedQueueSize);

            foreach (Action<RosTopicState> topicCallback in m_NewTopicCallbacks)
            {
                topicCallback(info);
            }
        }

        // Implement a service in Unity
        public void ImplementService<TRequest, TResponse>(string topic, Func<TRequest, Task<TResponse>> callback, int? queueSize = null)
            where TRequest : Message
            where TResponse : Message
        {
            string rosMessageName = rosMessageName = MessageRegistry.GetRosMessageName<TRequest>();

            RosTopicState info;
            if (!m_Topics.TryGetValue(topic, out info))
            {
                info = AddTopic(topic, rosMessageName, isService: true);
            }

            int resolvedQueueSize = queueSize.GetValueOrDefault(k_DefaultPublisherQueueSize);
            info.ImplementService(callback, resolvedQueueSize);

            foreach (Action<RosTopicState> topicCallback in m_NewTopicCallbacks)
            {
                topicCallback(info);
            }
        }

        // Send a request to a ros service
        public async void SendServiceMessage<RESPONSE>(string rosServiceName, Message serviceRequest, Action<RESPONSE> callback) where RESPONSE : Message, new()
        {
            RESPONSE response = await SendServiceMessage<RESPONSE>(rosServiceName, serviceRequest);
            try
            {
                callback(response);
            }
            catch (Exception e)
            {
                Debug.LogError("Exception in service callback: " + e);
            }
        }

        // Send a request to a ros service
        public async Task<RESPONSE> SendServiceMessage<RESPONSE>(string rosServiceName, Message serviceRequest) where RESPONSE : Message, new()
        {
            var serviceSerializer = new MessageSerializer();
            serviceSerializer.SerializeMessage(serviceRequest);
            byte[] requestBytes = serviceSerializer.GetBytes();
            TaskPauser pauser = new TaskPauser();

            int srvID;
            lock (m_ServiceRequestLock)
            {
                srvID = m_NextSrvID++;
                m_ServicesWaiting.Add(srvID, pauser);
            }

            RosTopicState topicState = GetOrCreateTopic(rosServiceName, serviceRequest.RosMessageName, isService: true);
            topicState.SendServiceRequest(serviceRequest, srvID);

            byte[] rawResponse = (byte[])await pauser.PauseUntilResumed();

            topicState.OnMessageReceived(rawResponse);
            RESPONSE result = m_MessageDeserializer.DeserializeMessage<RESPONSE>(rawResponse);
            return result;
        }

        public void GetTopicList(Action<string[]> callback)
        {
            m_TopicsListCallbacks.Add(callback);
            SendSysCommand(SysCommand.k_SysCommand_TopicList, new SysCommand_TopicsRequest());
        }

        public void GetTopicAndTypeList(Action<Dictionary<string, string>> callback)
        {
            m_TopicsAndTypesListCallbacks.Add(callback);
            SendSysCommand(SysCommand.k_SysCommand_TopicList, new SysCommand_TopicsRequest());
        }

        [Obsolete("Calling Subscribe now implicitly registers a subscriber")]
        public void RegisterSubscriber(string topic, string rosMessageName)
        {
        }

        public RosTopicState RegisterPublisher<T>(string rosTopicName,
            int? queue_size = null, bool? latch = null) where T : Message
        {
            return RegisterPublisher(rosTopicName, MessageRegistry.GetRosMessageName<T>(), queue_size, latch);
        }

        public RosTopicState RegisterPublisher(string rosTopicName, string rosMessageName,
            int? queueSize = null, bool? latch = null)
        {
            RosTopicState topicState = GetOrCreateTopic(rosTopicName, rosMessageName);
            //Create a new publisher.
            int resolvedQueueSize = queueSize.GetValueOrDefault(k_DefaultPublisherQueueSize);
            bool resolvedLatch = latch.GetValueOrDefault(k_DefaultPublisherLatch);
            topicState.RegisterPublisher(resolvedQueueSize, resolvedLatch);
            return topicState;
        }

        public void RegisterRosService<TRequest, TResponse>(string topic) where TRequest : Message where TResponse : Message
        {
            RegisterRosService(topic, MessageRegistry.GetRosMessageName<TRequest>(), MessageRegistry.GetRosMessageName<TResponse>());
        }

        public void RegisterRosService(string topic, string requestMessageName, string responseMessageName, int? queueSize = null)
        {
            RosTopicState info = GetOrCreateTopic(topic, requestMessageName, isService: true);
            int resolvedQueueSize = queueSize.GetValueOrDefault(k_DefaultPublisherQueueSize);
            info.RegisterRosService(responseMessageName, resolvedQueueSize);
        }

        [Obsolete("Calling ImplementUnityService now implicitly registers it")]
        public void RegisterUnityService(string topic, string rosMessageName)
        {
        }

        internal struct InternalAPI
        {
            ROSConnection m_Self;

            public InternalAPI(ROSConnection self) { m_Self = self; }

            public MessageDeserializer Deserializer => m_Self.m_MessageDeserializer;

            public void SendSubscriberRegistration(string topic, string rosMessageName, NetworkStream stream = null)
            {
                m_Self.SendSysCommand(SysCommand.k_SysCommand_Subscribe, new SysCommand_TopicAndType { topic = topic, message_name = rosMessageName }, stream);
            }

            public void SendRosServiceRegistration(string topic, string rosMessageName, NetworkStream stream = null)
            {
                m_Self.SendSysCommand(SysCommand.k_SysCommand_RosService, new SysCommand_TopicAndType { topic = topic, message_name = rosMessageName }, stream);
            }

            public void SendUnityServiceRegistration(string topic, string rosMessageName, NetworkStream stream = null)
            {
                m_Self.SendSysCommand(SysCommand.k_SysCommand_UnityService, new SysCommand_TopicAndType { topic = topic, message_name = rosMessageName }, stream);
            }

            public void SendSubscriberUnregistration(string topic, NetworkStream stream = null)
            {
                m_Self.SendSysCommand(SysCommand.k_SysCommand_RemoveSubscriber, new SysCommand_Topic { topic = topic }, stream);
            }

            public void SendPublisherUnregistration(string topic, NetworkStream stream = null)
            {
                m_Self.SendSysCommand(SysCommand.k_SysCommand_RemovePublisher, new SysCommand_Topic { topic = topic }, stream);
            }

            public void SendRosServiceUnregistration(string topic, NetworkStream stream = null)
            {
                m_Self.SendSysCommand(SysCommand.k_SysCommand_RemoveRosService, new SysCommand_Topic { topic = topic }, stream);
            }

            public void SendUnityServiceUnregistration(string topic, NetworkStream stream = null)
            {
                m_Self.SendSysCommand(SysCommand.k_SysCommand_RemoveUnityService, new SysCommand_Topic { topic = topic }, stream);
            }

            public void SendUnityServiceResponse(int serviceId, NetworkStream stream = null)
            {
                m_Self.SendSysCommand(SysCommand.k_SysCommand_ServiceResponse, new SysCommand_Service { srv_id = serviceId }, stream);
            }

            public void SendPublisherRegistration(string topic, string message_name, int queueSize, bool latch, NetworkStream stream = null)
            {
                m_Self.SendSysCommand(SysCommand.k_SysCommand_Publish,
                    new SysCommand_PublisherRegistration { topic = topic, message_name = message_name, queue_size = queueSize, latch = latch }
                );
            }

            public void SendServiceRequest(int serviceId)
            {
                m_Self.SendSysCommand(SysCommand.k_SysCommand_ServiceRequest, new SysCommand_Service { srv_id = serviceId });
            }

            public void AddSenderToQueue(OutgoingMessageSender sender)
            {
                m_Self.m_OutgoingMessageQueue.Enqueue(sender);
            }
        }

        static ROSConnection _instance;

        public static ROSConnection GetOrCreateInstance()
        {
            if (_instance == null)
            {
                // Prefer to use the ROSConnection in the scene, if any
                _instance = FindObjectOfType<ROSConnection>();
                if (_instance != null)
                    return _instance;

                GameObject prefab = Resources.Load<GameObject>("ROSConnectionPrefab");
                if (prefab == null)
                {
                    Debug.LogWarning("No settings for ROSConnection.instance! Open \"ROS Settings\" from the Robotics menu to configure it.");
                    GameObject instance = new GameObject("ROSConnection");
                    _instance = instance.AddComponent<ROSConnection>();
                }
                else
                {
                    _instance = Instantiate(prefab).GetComponent<ROSConnection>();
                }
            }
            return _instance;
        }

        [Obsolete("Please call ROSConnection.GetOrCreateInstance()")]
        public static ROSConnection instance
        {
            get
            {
                return GetOrCreateInstance();
            }
        }

        void Awake()
        {
            if (_instance == null)
                _instance = this;
        }

        void Start()
        {
            InitializeHUD();

            HudPanel.RegisterHeader(DrawHeaderGUI);

            if (listenForTFMessages)
                TFSystem.GetOrCreateInstance();

            if (ConnectOnStart)
                Connect();
        }

        public void Connect(string ipAddress, int port)
        {
            RosIPAddress = ipAddress;
            RosPort = port;
            Connect();
        }

        public void Connect()
        {
            if (!IPFormatIsCorrect(RosIPAddress))
                Debug.LogWarning("Invalid ROS IP address: " + RosIPAddress);

            m_ConnectionThreadCancellation = new CancellationTokenSource();

            Task.Run(() => ConnectionThread(
                RosIPAddress,
                RosPort,
                m_NetworkTimeoutSeconds,
                m_KeepaliveTime,
                (int)(m_SleepTimeSeconds * 1000.0f),
                OnConnectionStartedCallback,
                OnConnectionLostCallback,
                m_OutgoingMessageQueue,
                m_IncomingMessages,
                m_ConnectionThreadCancellation.Token
            ));
        }

        // NB this callback is not running on the main thread, be cautious about modifying data here
        void OnConnectionStartedCallback(NetworkStream stream)
        {
            RosTopicState[] topics;
            lock (m_Topics)
            {
                topics = AllTopics.ToArray();
            }

            foreach (RosTopicState topicInfo in m_Topics.Values.ToArray())
                topicInfo.OnConnectionEstablished(stream);

            RefreshTopicsList();
        }

        void OnConnectionLostCallback()
        {
            RosTopicState[] topics;
            lock (m_Topics)
            {
                topics = AllTopics.ToArray();
            }

            foreach (RosTopicState topicInfo in topics)
            {
                //For all publishers, notify that they need to re-register.
                topicInfo.OnConnectionLost();
            }
        }

        public void Disconnect()
        {
            m_ConnectionThreadCancellation?.Cancel();
            //The thread may be waiting on a ManualResetEvent, if so, this will wake it so it can exit immediately.
            m_OutgoingMessageQueue?.NewMessageReadyToSendEvent?.Set();
            m_ConnectionThreadCancellation = null;
        }

        void OnValidate()
        {
            // the prefab is not the instance!
            if (gameObject.scene.name == null)
                return;

            if (_instance == null)
                _instance = this;
        }

        Action<string, byte[]> m_SpecialIncomingMessageHandler;

        void Update()
        {
            s_RealTimeSinceStartup = Time.realtimeSinceStartup;

            Tuple<string, byte[]> data;
            while (m_IncomingMessages.TryDequeue(out data))
            {
                (string topic, byte[] contents) = data;
                m_LastMessageReceivedRealtime = Time.realtimeSinceStartup;

                if (m_SpecialIncomingMessageHandler != null)
                {
                    m_SpecialIncomingMessageHandler(topic, contents);
                }
                else if (topic.StartsWith("__"))
                {
                    ReceiveSysCommand(topic, Encoding.UTF8.GetString(contents));
                }
                else
                {
                    RosTopicState topicInfo = GetTopic(topic);
                    // if this is null, we have received a message on a topic we've never heard of...!?
                    // all we can do is ignore it, we don't even know what type it is
                    if (topicInfo != null)
                    {
                        try
                        {
                            //Add a try catch so that bad logic from one received message doesn't
                            //cause the Update method to exit without processing other received messages.
                            topicInfo.OnMessageReceived(contents);
                        }
                        catch (Exception e)
                        {
                            Debug.LogException(e);
                        }

                    }
                }
            }
        }

        float m_LastTopicsRequestRealtime = -1;
        const float k_TimeBetweenTopicsUpdates = 5.0f;

        public void RefreshTopicsList()
        {
            // cap the rate of requests
            if (m_LastTopicsRequestRealtime != -1 && s_RealTimeSinceStartup - m_LastTopicsRequestRealtime <= k_TimeBetweenTopicsUpdates)
                return;

            m_LastTopicsRequestRealtime = s_RealTimeSinceStartup;
            GetTopicAndTypeList((data) =>
            {
                foreach (KeyValuePair<string, string> kv in data)
                {
                    RosTopicState state = GetOrCreateTopic(kv.Key, kv.Value);
                    if (state.RosMessageName != kv.Value)
                    {
                        state.ChangeRosMessageName(kv.Value);
                    }
                }
            });
        }

        void ReceiveSysCommand(string topic, string json)
        {
            switch (topic)
            {
                case SysCommand.k_SysCommand_Handshake:
                    {
                        var handshakeCommand = JsonUtility.FromJson<SysCommand_Handshake>(json);
                        if (handshakeCommand.version == null)
                        {
                            Debug.LogError($"Corrupted or unreadable ROS-TCP-Endpoint version data! Expected: {k_Version}");
                        }
                        else if (!handshakeCommand.version.StartsWith(k_CompatibleVersionPrefix))
                        {
                            Debug.LogError($"Incompatible ROS-TCP-Endpoint version: {handshakeCommand.version}. Expected: {k_Version}");
                        }

                        var handshakeMetadata = JsonUtility.FromJson<SysCommand_Handshake_Metadata>(handshakeCommand.metadata);
#if ROS2
                        if (handshakeMetadata.protocol != "ROS2")
                        {
                            Debug.LogError($"Incompatible protocol: ROS-TCP-Endpoint is using {handshakeMetadata.protocol}, but Unity is in ROS2 mode. Switch it from the Robotics/Ros Settings menu.");
                        }
#else
                        if (handshakeMetadata.protocol != "ROS1")
                        {
                            Debug.LogError($"Incompatible protocol: ROS-TCP-Endpoint is using {handshakeMetadata.protocol}, but Unity is in ROS1 mode. Switch it from the Robotics/Ros Settings menu.");
                        }
#endif
                    }
                    break;
                case SysCommand.k_SysCommand_Log:
                    {
                        var logCommand = JsonUtility.FromJson<SysCommand_Log>(json);
                        Debug.Log(logCommand.text);
                    }
                    break;
                case SysCommand.k_SysCommand_Warning:
                    {
                        var logCommand = JsonUtility.FromJson<SysCommand_Log>(json);
                        Debug.LogWarning(logCommand.text);
                    }
                    break;
                case SysCommand.k_SysCommand_Error:
                    {
                        var logCommand = JsonUtility.FromJson<SysCommand_Log>(json);
                        Debug.LogError(logCommand.text);
                    }
                    break;
                case SysCommand.k_SysCommand_ServiceRequest:
                    {
                        var serviceCommand = JsonUtility.FromJson<SysCommand_Service>(json);

                        // the next incoming message will be a request for a Unity service, so set a special callback to process it
                        m_SpecialIncomingMessageHandler = (string serviceTopic, byte[] requestBytes) =>
                        {
                            m_SpecialIncomingMessageHandler = null;

                            RosTopicState topicState = GetTopic(serviceTopic);
                            if (topicState == null)
                            {
                                Debug.LogError($"Unity service {serviceTopic} has not been implemented!");
                                return;
                            }

                            topicState.HandleUnityServiceRequest(requestBytes, serviceCommand.srv_id);
                        };
                    }
                    break;

                case SysCommand.k_SysCommand_ServiceResponse:
                    {
                        // the next incoming message will be a response from a ros service
                        var serviceCommand = JsonUtility.FromJson<SysCommand_Service>(json);
                        m_SpecialIncomingMessageHandler = (string serviceTopic, byte[] requestBytes) =>
                        {
                            m_SpecialIncomingMessageHandler = null;

                            TaskPauser resumer;
                            lock (m_ServiceRequestLock)
                            {
                                if (!m_ServicesWaiting.TryGetValue(serviceCommand.srv_id, out resumer))
                                {
                                    Debug.LogError($"Unable to route service response on \"{serviceTopic}\"! SrvID {serviceCommand.srv_id} does not exist.");
                                    return;
                                }

                                m_ServicesWaiting.Remove(serviceCommand.srv_id);
                            }
                            resumer.Resume(requestBytes);
                        };
                    }
                    break;

                case SysCommand.k_SysCommand_TopicList:
                    {
                        var topicsResponse = JsonUtility.FromJson<SysCommand_TopicsResponse>(json);
                        if (m_TopicsAndTypesListCallbacks.Count > 0)
                        {
                            Dictionary<string, string> callbackParam = new Dictionary<string, string>();
                            for (int idx = 0; idx < topicsResponse.topics.Length; ++idx)
                                callbackParam[topicsResponse.topics[idx]] = topicsResponse.types[idx];
                            m_TopicsAndTypesListCallbacks.ForEach(a => a(callbackParam));
                            m_TopicsAndTypesListCallbacks.Clear();
                        }
                        if (m_TopicsListCallbacks.Count > 0)
                        {
                            m_TopicsListCallbacks.ForEach(a => a(topicsResponse.topics));
                            m_TopicsListCallbacks.Clear();
                        }
                    }
                    break;
            }
        }

        static void SendKeepalive(NetworkStream stream)
        {
            // 8 zeroes = a ros message with topic "" and no message data.
            stream.Write(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 }, 0, 8);
        }

        static void ClearMessageQueue(OutgoingMessageQueue queue)
        {
            while (queue.TryDequeue(out OutgoingMessageSender sendsOutgoingMessages))
            {
                sendsOutgoingMessages.ClearAllQueuedData();
            }
        }

        // How long a connection may go without progress before we drop it and reconnect.
        // Used for blocking writes, and for silence from an endpoint that sends keepalives.
        // Matches the endpoint's default TCP_CLIENT_TIMEOUT.
        const float k_DeadConnectionTimeoutSeconds = 10.0f;
        const float k_ConnectTimeoutSeconds = 5.0f;

        // Shared between the reader task and the connection thread of one connection.
        class ConnectionHealth
        {
            // Stopwatch.GetTimestamp() of the last message read from the endpoint (or of the connect).
            long m_LastReceiveTimestamp = Stopwatch.GetTimestamp();
            volatile bool m_ReceivedAnything;
            // The fork's endpoint sends an empty keepalive message every second when idle.
            // Upstream endpoints never do, so after the handshake the silence check is only
            // enabled once one has been seen.
            volatile bool m_EndpointSendsKeepalives;

            public void OnMessageRead(bool isKeepalive)
            {
                Interlocked.Exchange(ref m_LastReceiveTimestamp, Stopwatch.GetTimestamp());
                m_ReceivedAnything = true;
                if (isKeepalive)
                    m_EndpointSendsKeepalives = true;
            }

            public bool EndpointWentSilent(float timeoutSeconds)
            {
                // Every endpoint sends its handshake as soon as it accepts, so no handshake
                // within the timeout means the endpoint is stuck, whatever its version.
                if (m_ReceivedAnything && !m_EndpointSendsKeepalives)
                    return false;
                long elapsed = Stopwatch.GetTimestamp() - Interlocked.Read(ref m_LastReceiveTimestamp);
                return elapsed > (long)(timeoutSeconds * Stopwatch.Frequency);
            }
        }

        static async Task ConnectionThread(
            string rosIPAddress,
            int rosPort,
            float networkTimeoutSeconds,
            float keepaliveTime,
            int sleepMilliseconds,
            Action<NetworkStream> OnConnectionStartedCallback,
            Action DeregisterAll,
            OutgoingMessageQueue outgoingQueue,
            ConcurrentQueue<Tuple<string, byte[]>> incomingQueue,
            CancellationToken token)
        {
            //Debug.Log("ConnectionThread begins");
            int nextReaderIdx = 101;
            int nextReconnectionDelay = 1000;
            MessageSerializer messageSerializer = new MessageSerializer();
            // Only warn once per outage instead of every reconnection attempt.
            bool loggedConnectionFailure = false;

            while (!token.IsCancellationRequested)
            {
                TcpClient client = null;
                CancellationTokenSource readerCancellation = null;
                CancellationTokenRegistration closeOnCancel = default;

                try
                {
                    ROSConnection.m_HasConnectionError = true; // until we actually see a reply back, assume there's a problem

                    client = new TcpClient();
                    client.NoDelay = true;

                    // Disconnect() cancels the token from the main thread. Closing the socket is
                    // the only reliable way to wake this thread and the reader if they are blocked
                    // in Connect, Write or ReadAsync (Mono's ReadAsync ignores the token once the
                    // read is pending).
                    TcpClient clientToClose = client;
                    closeOnCancel = token.Register(() => clientToClose.Close());

                    // Connect with a timeout. A plain Connect() to a host that is down can block
                    // for minutes (SYN retries), during which we can't notice the host coming back.
                    Task connectTask = client.ConnectAsync(rosIPAddress, rosPort);
                    if (await Task.WhenAny(connectTask, Task.Delay((int)(k_ConnectTimeoutSeconds * 1000), token)) != connectTask)
                    {
                        // observe the exception the abandoned connect will throw when the client is closed
                        _ = connectTask.ContinueWith(t => t.Exception, TaskContinuationOptions.OnlyOnFaulted);
                        token.ThrowIfCancellationRequested();
                        throw new TimeoutException($"no answer after {k_ConnectTimeoutSeconds} s");
                    }
                    await connectTask; // rethrows connection refused etc.

                    NetworkStream networkStream = client.GetStream();
                    networkStream.ReadTimeout = (int)(networkTimeoutSeconds * 1000);
                    // Without this, Write() blocks forever once the send buffer fills up on a
                    // half-open connection, and the connection thread never reconnects.
                    networkStream.WriteTimeout = (int)(k_DeadConnectionTimeoutSeconds * 1000);

                    SendKeepalive(networkStream);
                    OnConnectionStartedCallback(networkStream);

                    ConnectionHealth health = new ConnectionHealth();
                    readerCancellation = new CancellationTokenSource();
                    int readerIdx = nextReaderIdx;
                    Task readerTask = Task.Run(() => ReaderThread(readerIdx, networkStream, incomingQueue, sleepMilliseconds, health, readerCancellation.Token));
                    nextReaderIdx++;

                    // Wall clock for keepalives. s_RealTimeSinceStartup is only updated in Update(),
                    // so it stops while the editor is paused or the main thread is busy, and we
                    // stopped sending keepalives exactly when the endpoint needed them.
                    Stopwatch sinceLastSend = Stopwatch.StartNew();

                    // connected, now just watch our queue for outgoing messages to send (or else send a keepalive message occasionally)
                    while (true)
                    {
                        bool messageReadyEventWasSet = outgoingQueue.NewMessageReadyToSendEvent.WaitOne(sleepMilliseconds);
                        token.ThrowIfCancellationRequested();

                        // The reader stops on any read error, including the endpoint closing the
                        // connection. Reconnect instead of writing into a dead socket.
                        if (readerTask.IsCompleted)
                            throw new System.IO.IOException("connection closed by the ROS endpoint");

                        if (health.EndpointWentSilent(k_DeadConnectionTimeoutSeconds))
                            throw new TimeoutException($"nothing received from the ROS endpoint for {k_DeadConnectionTimeoutSeconds} s");

                        if (messageReadyEventWasSet)
                        {
                            outgoingQueue.NewMessageReadyToSendEvent.Reset();
                        }
                        else
                        {
                            if (sinceLastSend.Elapsed.TotalSeconds > keepaliveTime)
                            {
                                SendKeepalive(networkStream);
                                sinceLastSend.Restart();
                            }
                        }

                        while (outgoingQueue.TryDequeue(out OutgoingMessageSender sendsOutgoingMessages))
                        {
                            OutgoingMessageSender.SendToState sendToState = sendsOutgoingMessages.SendInternal(messageSerializer, networkStream);
                            switch (sendToState)
                            {
                                case OutgoingMessageSender.SendToState.Normal:
                                    //This is normal operation.
                                    break;
                                case OutgoingMessageSender.SendToState.Ignore:
                                    //The queue was empty due to dropped messages. Ignore silently.
                                    break;
                                case OutgoingMessageSender.SendToState.QueueFullWarning:
                                    //Unable to send messages to ROS as fast as we're generating them.
                                    //This could be caused by a TCP connection that is too slow.
                                    Debug.LogWarning($"Queue full! Messages are getting dropped! " +
                                                     "Try check your connection speed is fast enough to handle the traffic.");
                                    break;
                                case OutgoingMessageSender.SendToState.NoMessageToSendError:
                                    //This indicates
                                    Debug.LogError(
                                        "Logic Error! A 'SendsOutgoingMessages' was queued but did not have any messages to send.");
                                    break;
                            }

                            token.ThrowIfCancellationRequested();
                            sinceLastSend.Restart();
                        }

                        if (!ROSConnection.m_HasConnectionError)
                            loggedConnectionFailure = false; // got the handshake, so the next failure is a new outage
                    }
                }
                catch (OperationCanceledException)
                {
                }
                catch (System.Exception e)
                {
                    ROSConnection.m_HasConnectionError = true;
                    // Exceptions caused by Disconnect() closing the socket are not failures.
                    if (!token.IsCancellationRequested && !loggedConnectionFailure)
                    {
                        UnityEngine.Debug.LogWarning("Connection to " + rosIPAddress + ":" + rosPort + " failed or dropped, retrying every " +
                            (nextReconnectionDelay / 1000.0f) + " s - " + e.Message);
                        loggedConnectionFailure = true;
                    }
                }
                finally
                {
                    closeOnCancel.Dispose();

                    if (readerCancellation != null)
                        readerCancellation.Cancel();

                    // Closing the client also makes a pending ReadAsync in the reader throw, so the
                    // reader task always ends here.
                    if (client != null)
                        client.Close();

                    // clear the message queue
                    ClearMessageQueue(outgoingQueue);
                    // Must run after ClearMessageQueue: it marks subscribers as unregistered, so
                    // OnConnectionStartedCallback re-sends registrations that were just cleared.
                    DeregisterAll();
                }

                try
                {
                    if (!token.IsCancellationRequested)
                        await Task.Delay(nextReconnectionDelay, token);
                }
                catch (OperationCanceledException)
                {
                }
            }
        }

        static async Task ReaderThread(int readerIdx, NetworkStream networkStream, ConcurrentQueue<Tuple<string, byte[]>> queue, int sleepMilliseconds, ConnectionHealth health, CancellationToken token)
        {
            // Any read error ends this task, and the connection thread reconnects when it sees that.
            // (It used to log and retry forever, which spun on a closed socket.)
            try
            {
                // First message should be the handshake
                Tuple<string, byte[]> handshakeContent = await ReadMessageContents(networkStream, sleepMilliseconds, token);
                health.OnMessageRead(false);
                if (handshakeContent.Item1 == SysCommand.k_SysCommand_Handshake)
                {
                    ROSConnection.m_HasConnectionError = false;
                    queue.Enqueue(handshakeContent);
                }
                else
                {
                    Debug.LogError($"Invalid ROS-TCP-Endpoint version detected: 0.6.0 or older. Expected: {k_Version}.");
                }

                while (!token.IsCancellationRequested)
                {
                    Tuple<string, byte[]> content = await ReadMessageContents(networkStream, sleepMilliseconds, token);
                    ROSConnection.m_HasConnectionError = false;
                    bool isKeepalive = content.Item1 == "";
                    health.OnMessageRead(isKeepalive);

                    if (!isKeepalive) // ignore keepalive messages
                        queue.Enqueue(content);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception e)
            {
                // After Disconnect() or a reconnect the socket is closed on purpose; don't report that.
                if (!token.IsCancellationRequested)
                {
                    ROSConnection.m_HasConnectionError = true;
                    Debug.Log("Reader " + readerIdx + " stopped: " + e.Message);
                }
            }
        }

        static async Task ReadToByteArray(NetworkStream networkStream, byte[] array, int length, int sleepMilliseconds, CancellationToken token)
        {
            // No more DataAvailable polling with Task.Delay: ReadAsync waits for data by itself.
            // Cancellation works by closing the socket (see ConnectionThread), which makes a
            // pending ReadAsync throw.
            int read = 0;
            while (read < length && networkStream.CanRead)
            {
                token.ThrowIfCancellationRequested();
                int n = await networkStream.ReadAsync(array, read, length - read, token);
                // 0 bytes means the endpoint closed the connection. Without this check the loop
                // did read += 0 forever at 100% CPU.
                if (n == 0)
                    throw new System.IO.IOException("the ROS endpoint closed the connection");
                read += n;
            }

            if (read < length)
                throw new SocketException(); // the connection has closed
        }

        static byte[] s_FourBytes = new byte[4];
        static byte[] s_TopicScratchSpace = new byte[64];

        static async Task<Tuple<string, byte[]>> ReadMessageContents(NetworkStream networkStream, int sleepMilliseconds, CancellationToken token)
        {
            // Get first bytes to determine length of topic name
            await ReadToByteArray(networkStream, s_FourBytes, 4, sleepMilliseconds, token);
            int topicLength = BitConverter.ToInt32(s_FourBytes, 0);

            // If our topic buffer isn't large enough, make a larger one (and keep it that size; assume that's the new standard)
            if (topicLength > s_TopicScratchSpace.Length)
                s_TopicScratchSpace = new byte[topicLength];

            // Read and convert topic name
            await ReadToByteArray(networkStream, s_TopicScratchSpace, topicLength, sleepMilliseconds, token);
            string topicName = Encoding.ASCII.GetString(s_TopicScratchSpace, 0, topicLength);

            await ReadToByteArray(networkStream, s_FourBytes, 4, sleepMilliseconds, token);
            int full_message_size = BitConverter.ToInt32(s_FourBytes, 0);

            // Exact size, not ArrayPool.Rent(): Rent can return a bigger array, and the buffer is
            // handed to code that uses its Length (Encoding.UTF8.GetString for sys commands,
            // user callbacks via RosTopicState). The trailing bytes would end up in the JSON.
            // The buffer is never returned to a pool either, since it outlives this method.
            byte[] readBuffer = new byte[full_message_size];
            await ReadToByteArray(networkStream, readBuffer, full_message_size, sleepMilliseconds, token);

            return Tuple.Create(topicName, readBuffer);
        }

        void OnApplicationQuit()
        {
            Disconnect();
        }

        void SendSysCommand(string command, object param, NetworkStream stream = null)
        {
            if (stream != null)
                SendSysCommandImmediate(command, param, stream);
            else
                QueueSysCommand(command, param);
        }

        static void PopulateSysCommand(MessageSerializer messageSerializer, string command, object param)
        {
            messageSerializer.Clear();
            // syscommands are sent as:
            // 4 byte command length, followed by that many bytes of the command
            // (all command names start with __ to distinguish them from ros topics)
            messageSerializer.Write(command);
            // 4-byte json length, followed by a json string of that length
            string json = JsonUtility.ToJson(param);
            messageSerializer.WriteUnaligned(json);
        }

        static void SendSysCommandImmediate(string command, object param, NetworkStream stream)
        {
            if (stream == null)
            {
                throw new ArgumentException("stream cannot be null!");
            }
            MessageSerializer messageSerializer = new MessageSerializer();
            PopulateSysCommand(messageSerializer, command, param);
            messageSerializer.SendTo(stream);
        }

        public void QueueSysCommand(string command, object param)
        {
            PopulateSysCommand(m_MessageSerializer, command, param);
            m_OutgoingMessageQueue.Enqueue(new SysCommandSender(m_MessageSerializer.GetBytesSequence()));
        }

        [Obsolete("Use Publish instead of Send", false)]
        public void Send(string rosTopicName, Message message)
        {
            Publish(rosTopicName, message);
        }

        public void Publish(string rosTopicName, Message message)
        {
            if (rosTopicName.StartsWith("__"))
            {
                QueueSysCommand(rosTopicName, message);
            }
            else
            {
                RosTopicState rosTopic = GetTopic(rosTopicName);
                if (rosTopic == null || !rosTopic.IsPublisher)
                {
                    throw new Exception($"No registered publisher on topic {rosTopicName} for type {message.RosMessageName}!");
                }

                rosTopic.Publish(message);
            }
        }

        void InitializeHUD()
        {
            if (!Application.isPlaying || (!m_ShowHUD && m_HudPanel == null))
                return;

            if (m_HudPanel == null)
            {
                m_HudPanel = gameObject.AddComponent<HudPanel>();
            }

            m_HudPanel.isEnabled = m_ShowHUD;
        }

        void DrawHeaderGUI()
        {
            GUIStyle labelStyle = new GUIStyle
            {
                alignment = TextAnchor.MiddleLeft,
                normal = { textColor = Color.white },
                fontStyle = FontStyle.Bold,
                fixedWidth = 250
            };

            GUIStyle contentStyle = new GUIStyle
            {
                alignment = TextAnchor.MiddleLeft,
                padding = new RectOffset(10, 0, 0, 5),
                normal = { textColor = Color.white },
                fixedWidth = 300
            };


            // ROS IP Setup
            GUILayout.BeginHorizontal();
            DrawConnectionArrows(
                true,
                0,
                0,
                Time.realtimeSinceStartup - LastMessageReceivedRealtime,
                Time.realtimeSinceStartup - LastMessageSentRealtime,
                HasConnectionThread,
                HasConnectionThread,
                HasConnectionError
            );

#if ROS2
            string protocolName = "ROS2";
#else
            string protocolName = "ROS";
#endif

            GUILayout.Space(30);
            GUILayout.Label($"{protocolName} IP: ", labelStyle, GUILayout.Width(100));

            if (!HasConnectionThread)
            {
                // if you've never run a build on this machine before, initialize the playerpref settings to the ones from the RosConnection
                if (!PlayerPrefs.HasKey(PlayerPrefsKey_ROS_IP))
                    SetIPPref(RosIPAddress);
                if (!PlayerPrefs.HasKey(PlayerPrefsKey_ROS_TCP_PORT))
                    SetPortPref(RosPort);

                // NB, here the user is editing the PlayerPrefs values, not the ones in the RosConnection.
                // (So that the hud remembers what IP you used last time you ran this build.)
                // The RosConnection receives the edited values when you click Connect.
                SetIPPref(GUILayout.TextField(RosIPAddressPref));
                SetPortPref(Convert.ToInt32(GUILayout.TextField(RosPortPref.ToString())));

                GUILayout.EndHorizontal();
                GUILayout.Label("(Not connected)");
                if (GUILayout.Button("Connect"))
                    Connect(RosIPAddressPref, RosPortPref);
            }
            else
            {
                GUILayout.Label($"{RosIPAddress}:{RosPort}", contentStyle);
                GUILayout.EndHorizontal();
            }
        }

        static GUIStyle s_ConnectionArrowStyle;

        public static void DrawConnectionArrows(bool withBar, float x, float y, float receivedTime, float sentTime, bool isPublisher, bool isSubscriber, bool hasError)
        {
            if (s_ConnectionArrowStyle == null)
            {
                s_ConnectionArrowStyle = new GUIStyle
                {
                    alignment = TextAnchor.MiddleLeft,
                    normal = { textColor = Color.white },
                    fontSize = 22,
                    fontStyle = FontStyle.Bold,
                    fixedWidth = 250
                };
            }

            var baseColor = GUI.color;
            GUI.color = Color.white;
            if (withBar)
                GUI.Label(new Rect(x + 4, y + 5, 25, 15), "I", s_ConnectionArrowStyle);
            GUI.color = GetConnectionColor(receivedTime, isSubscriber, hasError);
            GUI.Label(new Rect(x + 8, y + 6, 25, 15), "\u2190", s_ConnectionArrowStyle);
            GUI.color = GetConnectionColor(sentTime, isPublisher, hasError);
            GUI.Label(new Rect(x + 8, y + 0, 25, 15), "\u2192", s_ConnectionArrowStyle);
            GUI.color = baseColor;
        }

        public static void SetIPPref(string ipAddress)
        {
            PlayerPrefs.SetString(PlayerPrefsKey_ROS_IP, ipAddress);
        }

        public static void SetPortPref(int port)
        {
            PlayerPrefs.SetInt(PlayerPrefsKey_ROS_TCP_PORT, port);
        }

        public static Color GetConnectionColor(float elapsedTime, bool hasConnection, bool hasError)
        {
            var bright = new Color(1, 1, 0.5f);
            var mid = new Color(0, 1, 1);
            var dark = new Color(0, 0.5f, 1);
            const float brightDuration = 0.03f;
            const float fadeToDarkDuration = 1.0f;

            if (!hasConnection)
                return Color.gray;
            if (hasError)
                return Color.red;

            if (elapsedTime <= brightDuration)
                return bright;
            return Color.Lerp(mid, dark, elapsedTime / fadeToDarkDuration);
        }

        public static bool IPFormatIsCorrect(string ipAddress)
        {
            if (ipAddress == null || ipAddress == "")
                return false;

            // If IP address is set using static lookup tables https://man7.org/linux/man-pages/man5/hosts.5.html
            if (Char.IsLetter(ipAddress[0]))
            {
                foreach (Char subChar in ipAddress)
                {
                    if (!(Char.IsLetterOrDigit(subChar) || subChar == '-' || subChar == '.'))
                        return false;
                }

                if (!Char.IsLetterOrDigit(ipAddress[ipAddress.Length - 1]))
                    return false;
                return true;
            }

            string[] subAdds = ipAddress.Split('.');
            if (subAdds.Length != 4)
            {
                return false;
            }
            IPAddress parsedipAddress;
            return IPAddress.TryParse(ipAddress, out parsedipAddress);
        }
    }
}
