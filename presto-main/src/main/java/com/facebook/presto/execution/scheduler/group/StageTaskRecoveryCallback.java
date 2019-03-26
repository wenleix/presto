package com.facebook.presto.execution.scheduler.group;

import com.facebook.presto.execution.TaskId;

public interface StageTaskRecoveryCallback
{
    void recover(TaskId taskId);
}
