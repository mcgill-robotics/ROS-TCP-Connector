using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;
using UnityEngine;

namespace Unity.Robotics.ROSTCPConnector
{
    public class TopicMessageSender : OutgoingMessageSender
    {
        public string RosMessageName { get; private set; }

        public string TopicName { get; private set; }

        public int QueueSize { get; private set; }

        //Messages waiting to be sent queue.
        LinkedList<Message> m_OutgoingMessages = new LinkedList<Message>();

        //Latching - This message will be set if latching is enabled, and on reconnection, it will be sent again.
        Message m_LastMessageSent = null;

        //Optional, used if you want to pool messages and reuse them when they are no longer in use.
        IMessagePool m_MessagePool;
        public bool MessagePoolEnabled => m_MessagePool != null;

        int m_DroppedMessageCount = 0;

        public TopicMessageSender(string topicName, string rosMessageName, int queueSize = 10)
        {
            if (queueSize < 1)
            {
                throw new Exception("Queue size must be greater than or equal to 1.");
            }

            TopicName = topicName;
            RosMessageName = rosMessageName;
            QueueSize = queueSize;
        }

        internal void Queue(Message message)
        {
            lock (m_OutgoingMessages)
            {
                if (m_OutgoingMessages.Count >= QueueSize)
                {
                    TryRecycleMessage(m_OutgoingMessages.First.Value);
                    m_OutgoingMessages.RemoveFirst();
                    
                    m_DroppedMessageCount++;
                    if (m_DroppedMessageCount % 30 == 1) // Log every 30 drops to avoid spam
                    {
                        UnityEngine.Debug.LogWarning($"[ROS-TCP-Connector] Dropped {m_DroppedMessageCount} messages on topic '{TopicName}'! " +
                                                     $"The ROS 2 endpoint cannot keep up with the publish rate.");
                    }
                }

                //Add a new valid message to the end.
                m_OutgoingMessages.AddLast(message);
            }
        }

        SendToState GetMessageToSend(out Message messageToSend)
        {
            SendToState result = SendToState.Ignore;
            messageToSend = null;
            lock (m_OutgoingMessages)
            {
                if (m_OutgoingMessages.Count > 0)
                {
                    messageToSend = m_OutgoingMessages.First.Value;
                    m_OutgoingMessages.RemoveFirst();
                    result = SendToState.Normal;
                }
            }

            return result;
        }

        public bool PeekNextMessageToSend(out Message messageToSend)
        {
            bool result = false;
            messageToSend = null;
            lock (m_OutgoingMessages)
            {
                if (m_OutgoingMessages.Count > 0)
                {
                    messageToSend = m_OutgoingMessages.First.Value;
                    result = true;
                }
            }


            return result;
        }

        void SendMessageWithStream(MessageSerializer messageSerializer, Stream stream, Message message)
        {
            //Clear the serializer
            messageSerializer.Clear();
            //Prepare the data to send.
            messageSerializer.Write(TopicName);
            messageSerializer.SerializeMessageWithLength(message);
            //Send via the stream.
            messageSerializer.SendTo(stream);
        }

        public void PrepareLatchMessage()
        {
            if (m_LastMessageSent != null && !m_OutgoingMessages.Any())
            {
                //This topic is latching, so to mimic that functionality,
                // the last sent message is sent again with the new connection.
                m_OutgoingMessages.AddFirst(m_LastMessageSent);
            }
        }

        public override SendToState SendInternal(MessageSerializer messageSerializer, Stream stream)
        {
            SendToState sendToState = GetMessageToSend(out Message toSend);
            if (sendToState == SendToState.Normal)
            {
                SendMessageWithStream(messageSerializer, stream, toSend);

                //Recycle the message (if applicable).
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
            List<Message> toRecycle;
            lock (m_OutgoingMessages)
            {
                toRecycle = new List<Message>(m_OutgoingMessages);
                m_OutgoingMessages.Clear();
            }

            foreach (Message messageToRecycle in toRecycle)
            {
                TryRecycleMessage(messageToRecycle);
            }
        }

        void TryRecycleMessage(Message toRecycle)
        {
            if (m_MessagePool != null)
            {
                //Add the message back to the pool.
                m_MessagePool.AddMessage(toRecycle);
            }
        }

        public void SetMessagePool(IMessagePool messagePool)
        {
            m_MessagePool = messagePool;
        }
    }
}
