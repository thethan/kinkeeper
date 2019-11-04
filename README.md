#KinKEEPER
A basic implementation of kafka's consumer-group for kinesis.

##Goal of this package: 
1. Keep Track of the last partition key inside of a group. 
2. Register consumers inside of the group. 
3. Automatically deregister users in a group. 
4. Make sure that one consumer gets a partition at a time. 
