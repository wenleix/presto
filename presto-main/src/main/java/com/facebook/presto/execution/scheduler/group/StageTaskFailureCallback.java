package com.facebook.presto.execution.scheduler.group;

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.spi.PrestoException;

import java.util.function.Consumer;

public interface StageTaskFailureCallback
{
    void recoverTaskFailure(TaskId taskId, Consumer<PrestoException> onFailure, Exception originalTaskFailureException);
}
