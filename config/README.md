We have two set of env for two situation

1. we want to test run the full pipeline locally
=> Use .env.single-machine
=> Copy it to your .env

2. we want ot test run the full pipeline in distributed manner
=> Use the env stored on seperate environment
=> Copy it to your .env
=> .env.distributed is an example file of how env is structered in the seperate environment
=> **Do not expose this env file**