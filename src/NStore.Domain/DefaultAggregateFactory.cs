﻿using System;

namespace NStore.Domain
{
    public class DefaultAggregateFactory : IAggregateFactory
    {
        public T Create<T>() where T : IAggregate
        {
            var aggregate = Activator.CreateInstance<T>();
            return aggregate;
        }

        public IAggregate Create(Type aggregateType)
        {
            return (IAggregate)Activator.CreateInstance(aggregateType);
        }
    }
}