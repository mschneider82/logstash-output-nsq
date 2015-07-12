logstash-output-nsq
====================

NSQ output Plugin for Logstash. This output will produce messages to a nsq topic using nsq-ruby. 

For more information about NSQ, refer to this [documentation](http://nsq.io) 

Logstash Configuration
====================

Using a static NSQD:
====================

    output {
       nsq {
            nsqd => 127.0.0.1:4150"
            topic => "testtopic"
       }
    }



Using NSQLOOKUPD to find nsqd servers for the given topic:
====================

    output {
       nsq {
            nsqlookupd => ["127.0.0.1:4161","1.2.3.4:4161"]
            topic => "testtopic"
       }
    }

Dependencies
====================

* nsq-ruby
