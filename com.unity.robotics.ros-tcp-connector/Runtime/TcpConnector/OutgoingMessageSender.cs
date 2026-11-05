using System.Collections.Generic;
using System.IO;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;

namespace Unity.Robotics.ROSTCPConnector
{
    public abstract class OutgoingMessageSender
    {

        public enum SendToState
        {
            Normal,
            NoMessageToSendError,
            QueueFullWarning,
            Ignore
        }

        public abstract SendToState SendInternal(MessageSerializer m_MessageSerializer, System.IO.Stream stream);

        public abstract void ClearAllQueuedData();
    }

    /**
     * Simple implementation of a OutgoingMessageSender that is used for sys commands
     * as they are handled differently to typical ROS messages and sent as JSON strings.
     */
    public class SysCommandSender : OutgoingMessageSender
    {
        List<SerializationChunk> m_Chunks;

        public SysCommandSender(List<SerializationChunk> chunks)
        {
            m_Chunks = chunks;
        }

        public override SendToState SendInternal(MessageSerializer m_MessageSerializer, Stream stream)
        {
            for (int i = 0; i < m_Chunks.Count; i++)
            {
                var chunk = m_Chunks[i];
                if (chunk.Buffer != null && chunk.Length > 0)
                {
                    stream.Write(chunk.Buffer, chunk.Offset, chunk.Length);
                }
            }

            return SendToState.Normal;
        }

        public override void ClearAllQueuedData()
        {
            m_Chunks.Clear();
        }
    }
}
