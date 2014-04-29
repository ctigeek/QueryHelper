QueryHelper
=================
This project was moved from CSharpQueryHelper.
It's now a library & nuget package. I renamed it to be more language agnostic.

Nuget page: https://www.nuget.org/packages/QueryHelper/

QueryHelper is a small lightweight library that utilizes functional constructs to help you run queries.
It manages all your sql connections, commands, parameters, and transactions.

All you do is supply: 
1. a connection string.
2. some SQL.

And the QueryHelper manages the connection, command, reader, and transaction.

It's fully mockable so unit testing is a breeze (especially compared with mocking a DbCommand!)

Sample code including unit tests:
https://github.com/ctigeek/QueryHelper/tree/master/Example

