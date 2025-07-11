call gradle clean uberJar examplesUberJar
native-image.cmd --install-exit-handlers -cp build\libs\chaos-runner-0.0.3-SNAPSHOT-uber.jar io.synadia.chaos.ChaosRunner chaos-runner