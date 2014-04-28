CSharpQueryHelper
=================

A class that utilizes functional constructs to help manage sql connections, commands, parameters, and transactions.

No nuget package needed. Just copy QueryHelper.cs into your solution.

All you do is supply: 
1. a connection string.
2. some SQL.

And the QueryHelper manages the connection, command, reader, and transaction.

It's fully mockable so unit testing is a breeze (especially compared with mocking a DbCommand!)

Sample code including unit tests:
https://github.com/ctigeek/CSharpQueryHelper/tree/master/CSharpQueryHelper/Example

