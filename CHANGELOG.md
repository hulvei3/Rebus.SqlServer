# Changelog

## 2.0.0-b01

* Test release

## 2.0.0-b02

* Use more convenient namespaces

## 2.0.0-b03

* Optimized streamed reading of data

## 2.0.0-b04

* Dispose connection properly also when an error occurs

## 2.0.0

* Release 2.0.0

## 2.1.0-b01

* Add support for schemas to all SQL Server-based things - thanks [cleytonb]

## 2.1.0-b02

* Topic/subscriber address length checks on subscription storage because SQL Server is crazy
* Support custom topic/subscriber address lengths in subscription storage via simple schema reflection at startup

## 3.0.0

* Update to Rebus 3

## 3.1.0

* Register SQL Transport as if it was an external timeout manager, enabling message deferral from one-way clients

## 3.1.1

* Fix unspecified saga revision after delete (fixes clash with SQL Server saga auditing assumption that revision be incremented also on delete...)

## 4.0.0

* Update to Rebus 4
* Add .NET Core support (netstandard1.6)
* Made `CurrentConnectionKey` of `SqlTransport` public

## 5.0.0-b4

* Add lease-based transport - thanks [MrMDavidson]
* Add creation time column to data bus storage - thanks [IsaacSee]
* Shift transports to use table-per-queue layout to improve overall performance and avoid having a long queue bring everything to a halt - thanks [magnus-tretton37]
* Change meaning of the secret and pretty un-documented `rbs2-msg-priority` header to work like it seems most people expect it to: Higher number means higher priority
* Change all `datetime2` column types to be `datetimeoffset` instead to make data less tied to the SQL Server instance (probably "server time of ...") that created it
* Retry schema generation if it fails, most likely because of race condition between checking for the existence of a table and trying to create it


----

[cleytonb]: https://github.com/cleytonb
[IsaacSee]: https://github.com/IsaacSee
[magnus-tretton37]: https://github.com/magnus-tretton37
[MrMDavidson]: https://github.com/MrMDavidson