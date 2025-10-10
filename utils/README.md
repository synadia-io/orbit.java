<img src="../orbit_shorter.png" alt="Orbit">

# Utilities

Miscellaneous Helper Code

### GenerateServerErrorConstants
`GenerateServerErrorConstants` will download the latest errors.json file and generate a java file like `ServerErrorConstants`

You can use whatever class name you want, 
but the generated class be kept in package `io.nats.client.api;` 
otherwise it cannot access the packaged scoped `io.nats.client.api.Error` class

---
Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
See [LICENSE](LICENSE) and [NOTICE](NOTICE) file for details.
