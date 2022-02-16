flink window可以按照time 与 count分为两类，timeWindow可按照事件事件也可按照处理时间，countWindow按照计数方式，当流入窗口的数据达到一定数据则会触发窗口函数。

       countWindow 与timeWindow一样需要Assigner、Trigger 等窗口组件，那么flink 是如何实现计数窗口，主要考虑两个问题：1. timeWindow 窗口分配有开始、结束时间来确定一个窗口，但是countWindow 如何确定一个窗口；2. 如何完成计数，以能够判断到达countWindow 的触发条件。其实这两个问题，通过Assigner、Trigger 来完美解决，以KeyedStream keyed流来分析查看countWindow 入口：


```
public WindowedStream<T, KEY, GlobalWindow> countWindow(

           long size) {

return window(GlobalWindows.create()).

trigger(PurgingTrigger.of(CountTrigger.of(size)));

}
```


通过GlobalWindows.create() 得到一个GlobaWindows 作为窗口的Assigner,

其窗口分配机制：



public Collection<GlobalWindow> assignWindows(

Object element, long timestamp,

WindowAssignerContext context) {

return Collections.singletonList(GlobalWindow.get());
}


可以看出一个元素只能属于一个窗口，GlobalWindow使用单例模式调用GlobalWindow.get 得到一个全局的GlobalWindow对象，即所有相同key的元素都属于一个同一个window对象，这是与timeWindow的区别，为什么所有的相同的key都是用同一个window对象，其实这是flink 对于状态使用做的一个优化，上篇文章中讲到窗口的中间数据是存储在状态中,一个operator状态的唯一性通过StateDesc、Key、Namespace, 窗口中namespace 就是window, 在中间数据存储会根据当前key与namespace 获取对应的状态List,如果不存在当前key、或者namespace就会new 一个list, 如果key 对应的每个窗口都是用相同的namespace, 那么就可以实现list 复用，也正是countWindow 使用全局唯一GlobalWindow的原因。

在来看第二个问题如何实现计数，countWindow 使用PurgingTrigger 作为其触发器，其内部封装了CountTrigger ，真正调用的是通过CountTrigger来进行计数与触发：

public TriggerResult onElement(Object element,

          long timestamp, W window, TriggerContext ctx)

throws Exception {

ReducingState<Long> count =

ctx.getPartitionedState(stateDesc);

count.add(1L);
if (count.get() >= maxCount) {
count.clear();
return TriggerResult.FIRE;
}
return TriggerResult.CONTINUE;
}
首先获取ReduceState 计数器，表示当前key 的数量，每处理一条数据就进行+1操作， 当count 达到执行触发量就会将当前key 计数state 清空，下次从0开始计数，并且触发窗口操作。这种状态计数器也会在checkpoint时候被储存，使其具有容错性，能够在任务失败中恢复，达到精确计数。

