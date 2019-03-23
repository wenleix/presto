package com.facebook.presto.spi;

public class PageSinkProperty
{
    private final boolean lifespanCommitRequired;

    private PageSinkProperty(boolean lifespanCommitRequired)
    {
        this.lifespanCommitRequired = lifespanCommitRequired;
    }

    public boolean isLifespanCommitRequired()
    {
        return lifespanCommitRequired;
    }

    public static final class Builder
    {
        private boolean lifespanCommitRequired;

        public Builder() {}

        public Builder setLifespanCommitRequired(boolean lifespanCommitRequired)
        {
            this.lifespanCommitRequired = lifespanCommitRequired;
            return this;
        }

        public PageSinkProperty build()
        {
            return new PageSinkProperty(lifespanCommitRequired);
        }
    }
}
