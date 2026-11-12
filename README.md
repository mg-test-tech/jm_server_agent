The existing ServerAgent was built on the Hyperic Sigar library, which is no longer maintained or supported. 
Sigar is incompatible with modern JVMs and newer operating systems, leaving the agent unable to collect reliable 
metrics on current deployments. 
This plugin replaces Sigar with OSHI, a fully supported and actively maintained native system monitoring library, 
ensuring accurate and portable metrics collection across platforms and Java versions.

The ServerAgent created for my own use, if you want to make changes, feel free to fork and change it to your own requirements.
It is not official plugin and, please, do not expect official support.
