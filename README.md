# DXAPP GraphLoader
This is a graph loading library/application for some formats for the distrubuted in-memory system 
[DXRAM](https://github.com/hhu-bsinfo/dxram/) as part of my Bachelor thesis.

#SetUp
<details><summary>CLICK ME</summary><p>
To run this application, its recommended to edit the default configs of [DXRAM]()
First set *m_enabled* to *true* and set the amount of workers per peer.

```JSON
"JobComponent": {
      "m_enabled": true,
      "m_numWorkers": VALUE
},
```

If you encounter issues with messages not being delivered duo to messages being dropped 
then you can increase the duration to time out to give the application some time to send the data.

```JSON
"NetworkComponent": {
      "m_nioConfig": {
          "m_value": VALUE,
          "m_unit": "ms"
        }
}
```
</p>
</details>

#API
This app can be run as DXRAM Application or used as library.
# License
Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems.
Licensed under the [GNU General Public License](LICENSE.md).
