using System;
using System.IO;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;
using UnityEngine;

namespace Unity.Robotics.ROSTCPConnector
{
    public class TopicMessageSender : OutgoingMessageSender
    {
        public string RosMessageName { get; private set; }

        public string TopicName { get; private set; }

        public int QueueSize { get; private set; }

        // Messages waiting to be sent queue (fixed circular buffer, zero-alloc)
        readonly Message[] m_OutgoingBuffer;
        int m_Head;
        int m_Tail;
        int m_Count;
        readonly object m_Lock = new object();

        // Latching - This message will be set if latching is enabled, and on reconnection, it will be sent again.
        Message m_LastMessageSent = null;

        // Optional, used if you want to pool messages and reuse them when they are no longer in use.
        IMessagePool m_MessagePool;
        public bool MessagePoolEnabled => m_MessagePool != null;

        int m_DroppedMessageCount = 0;

        public bool HasPendingMessages
        {
            get
            {
                lock (m_Lock)
                {
                    return m_Count > 0;
                }
            }
        }

        public TopicMessageSender(string topicName, string rosMessageName, int queueSize = 10)
        {
            if (queueSize < 1)
            {
                throw new Exception("Queue size must be greater than or equal to 1.");
            }

            TopicName = topicName;
            RosMessageName = rosMessageName;
            QueueSize = queueSize;
            m_OutgoingBuffer = new Message[queueSize];
        }

        internal void Queue(Message message)
        {
            lock (m_Lock)
            {
                if (m_Count >= QueueSize)
                {
                    Message oldest = m_OutgoingBuffer[m_Head];
                    m_OutgoingBuffer[m_Head] = null;
                    m_Head = (m_Head + 1) % QueueSize;
                    m_Count--;

                    TryRecycleMessage(oldest);

                    if (ROSConnection.IsConnected)
                    {
                        m_DroppedMessageCount++;
                        if (m_DroppedMessageCount % 30 == 1) // Log every 30 drops to avoid spam
                        {
                            UnityEngine.Debug.LogWarning($"[ROS-TCP-Connector] Dropped {m_DroppedMessageCount} messages on topic '{TopicName}'! " +
                                                         $"The ROS 2 endpoint cannot keep up with the publish rate.");
                        }
                    }
                    else
                    {
                        m_DroppedMessageCount = 0;
                    }
                }

                // Add message to ring buffer
                m_OutgoingBuffer[m_Tail] = message;
                m_Tail = (m_Tail + 1) % QueueSize;
                m_Count++;
            }
        }

        SendToState GetMessageToSend(out Message messageToSend)
        {
            SendToState result = SendToState.Ignore;
            messageToSend = null;
            lock (m_Lock)
            {
                if (m_Count > 0)
                {
                    messageToSend = m_OutgoingBuffer[m_Head];
                    m_OutgoingBuffer[m_Head] = null;
                    m_Head = (m_Head + 1) % QueueSize;
                    m_Count--;
                    result = SendToState.Normal;
                }
            }

            return result;
        }

        public bool PeekNextMessageToSend(out Message messageToSend)
        {
            bool result = false;
            messageToSend = null;
            lock (m_Lock)
            {
                if (m_Count > 0)
                {
                    messageToSend = m_OutgoingBuffer[m_Head];
                    result = true;
                }
            }

            return result;
        }

        void SendMessageWithStream(MessageSerializer messageSerializer, Stream stream, Message message)
        {
            // Clear the serializer
            messageSerializer.Clear();
            // Prepare the data to send.
            messageSerializer.Write(TopicName);
            messageSerializer.SerializeMessageWithLength(message);
            // Send via the stream.
            messageSerializer.SendTo(stream);
        }

        public void PrepareLatchMessage()
        {
            if (m_LastMessageSent != null)
            {
                lock (m_Lock)
                {
                    if (m_Count == 0)
                    {
                        // This topic is latching, so to mimic that functionality,
                        // the last sent message is sent again with the new connection.
                        m_Head = (m_Head - 1 + QueueSize) % QueueSize;
                        m_OutgoingBuffer[m_Head] = m_LastMessageSent;
                        m_Count++;
                    }
                }
            }
        }

        public override SendToState SendInternal(MessageSerializer messageSerializer, Stream stream)
        {
            SendToState sendToState = GetMessageToSend(out Message toSend);
            if (sendToState == SendToState.Normal)
            {
                SendMessageWithStream(messageSerializer, stream, toSend);

                // Recycle the message (if applicable).
                if (m_LastMessageSent != null && m_LastMessageSent != toSend)
                {
                    TryRecycleMessage(m_LastMessageSent);
                }

                m_LastMessageSent = toSend;
            }

            return sendToState;
        }

        public override void ClearAllQueuedData()
        {
            Message[] toRecycle;
            int countToRecycle = 0;
            lock (m_Lock)
            {
                toRecycle = new Message[m_Count];
                while (m_Count > 0)
                {
                    toRecycle[countToRecycle++] = m_OutgoingBuffer[m_Head];
                    m_OutgoingBuffer[m_Head] = null;
                    m_Head = (m_Head + 1) % QueueSize;
                    m_Count--;
                }
                m_Head = 0;
                m_Tail = 0;
            }

            for (int i = 0; i < countToRecycle; i++)
            {
                TryRecycleMessage(toRecycle[i]);
            }
        }

        void TryRecycleMessage(Message toRecycle)
        {
            if (m_MessagePool != null && toRecycle != null)
            {
                // Add the message back to the pool.
                m_MessagePool.AddMessage(toRecycle);
            }
        }

        public void SetMessagePool(IMessagePool messagePool)
        {
            m_MessagePool = messagePool;
        }
    }
}
