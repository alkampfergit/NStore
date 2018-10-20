using System;
using System.Runtime.Serialization;
using System.Security.Permissions;

namespace NStore.Domain
{
    /// <summary>
    /// https://blogs.msdn.microsoft.com/agileer/2013/05/17/the-correct-way-to-code-a-custom-exception-class/
    /// </summary>
    [Serializable]
    public class AggregateAlreadyInitializedException : Exception
    {
        public Type AggregateType { get; }
        public string AggregateId { get; }

        public AggregateAlreadyInitializedException(Type aggregateType, string aggregateId)
            : base($"Aggregate {aggregateId} of type {aggregateType.Name} has already been initialized.")
        {
            AggregateType = aggregateType;
            AggregateId = aggregateId;
        }
        
        protected AggregateAlreadyInitializedException(SerializationInfo info, StreamingContext context)
        :base(info, context)
        {
            AggregateId = info.GetString("AggregateId");
            AggregateType = Type.GetType(info.GetString("AggregateType"));
        }
        
        [SecurityPermission(SecurityAction.Demand, SerializationFormatter = true)]
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
                throw new ArgumentNullException(nameof(info));
            
            info.AddValue("AggregateId", AggregateId);
            info.AddValue("AggregateType", AggregateType.FullName);
            
            base.GetObjectData(info, context);
        }
    }
}