Now Archived and Forked
=====
avrogato-erl will not be maintained in this repository going forward. Please use, create issues on, and make PRs to the fork of [avrogato-erl](https://github.com/ferd/avrogato-erl) located here.


Avrogato
=====

An OTP application that wraps the [erlavro](https://github.com/klarna/erlavro) library, while providing it with access to a schema registry. Supports both Confluent and OCF message formats.

Should I use this
-----------------

You'll want to use this library if you need schemas defined in a confluent registry when handling Avro data.

All events encoded will contain either OCF or Confluent headers, and the schema is to be specified with each encoding and decoding to know if there is a clash.

You also can only use this if your project does not rely on `erlavro` already, or relies on a compatible version. Otherwise who knows what might happen.

Using in Erlang
-----

You must use Erlang version 21.0 or newer.

Add to your project as a dependency in `rebar.config`:

```erlang
{deps, [
    {avrogato, {git, "https://github.com/postmates/avrogato-erl.git"}, {branch, "master"}}
]}.
```

Don't forget to add the application to your `.app.src` file's `applications` tuple.
All the schemas are available once the application is started (`application:ensure_all_started(avrogato)` if it does not auto-boot in your shell or release).

Specify the following OTP configuration values before application start to impact its behaviour:

- `{registry, "http://example.org:8080"}`: Specify the registry from which to synchronize and fetch content. Without defining this value, the application will not boot. Supports HTTPS and HTTP Basic Auth.
- `{prefetch, all | [<<"namespace.schema">>, ...]}`: Specify which schemas to load on-start (blocking operation, best effort). We recommend fetching only specific sets of them rather than `all` since the full set can take minutes. The objective should be to pay a synchronization cost at boot time to ensure no blocking once the app starts accepting traffic. Defaults to `[]`

Events can be encoded by specifying the schema and a list of keys and values for the record, and by specifying the schema name and version (according to the registry).

```erlang
2> Record = [
2>     {<<"source">>, <<"phoneproxy">>},
2>     {<<"ts">>, os:system_time(milli_seconds)},
2>     {<<"uuid">>, <<"123e4567-e89b-12d3-a456-426655440000">>},
2>     {<<"version">>, 1},
2>     {<<"data">>, [
2>         {<<"status">>, <<"failed">>},
2>         {<<"user_key_bytes">>, <<"123456">>},
2>         {<<"sequence_uuid">>, <<"123e4567-e89b-12d3-a456-426655440000">>},
2>         {<<"error_code">>, <<"30006">>}
2>     ]}
2> ].
[{<<"source">>,<<"phoneproxy">>},
 {<<"ts">>,1570040294933},
 {<<"uuid">>,<<"123e4567-e89b-12d3-a456-426655440000">>},
 {<<"version">>,1},
 {<<"data">>,
  [{<<"status">>,<<"failed">>},
   {<<"user_key_bytes">>,<<"123456">>},
   {<<"sequence_uuid">>,
    <<"123e4567-e89b-12d3-a456-426655440000">>},
   {<<"error_code">>,<<"30006">>}]}]

3> EncodedConfluent = avrogato:encode({<<"phoneproxy.sms_delivered">>, 4}, Record).
[<<0,0,0,0,238>>,
 [[["\f",<<"123456">>],
   ["H",<<"123e4567-e89b-12d3-a456-426655440000">>],
   [[2],["\n",<<"30006">>]],
   ["\f",<<"failed">>]],
  [[24],<<"phoneproxy">>],
  [170,184,141,219,177,91],
  ["H",<<"123e4567-e89b-12d3-a456-426655440000">>],
  [2]]]

4> EncodedOCF = avrogato:encode_ocf({<<"phoneproxy.sms_delivered">>, 4}, Record).
[[<<79,98,106,1>>,
  [<<3,208,12>>,
   <<20,97,118,114,111,46,99,111,100,101,99,8,110,117,108,
     108,22,97,118,114,111,46,115,99,...>>,
   0],
  <<105,108,73,94,172,186,186,216,119,31,133,223,52,122,14,
    39>>],
 [[2],
  [230,1],
  <<"\f123456H123e4567-e89b-12d3-a456-426655440000"...>>,
  <<105,108,73,94,172,186,186,216,119,31,133,223,52,122,
    14,39>>]]
```

The encoded terms both are instances of an `iolist()`, ready to be dumped to a file or over the network. `EncodedConfluent` uses the confluent format, and `EncodedOCF` uses the OCF header format. Both can represent the same record, and can be decoded by simply passing the record to the decoding function. Do note that the "version" passed (`4`) is the registry version of the specific schema, and not the Confluent ID, since OCF as a format would not really care about confluent IDs.

Since the header is included, no further identification is required, but you should still assert on which schema version you are operating:

```erlang
5> avrogato:decode({<<"phoneproxy.sms_delivered">>, 3}, EncodedConfluent).
{error,schema_mismatch}
6> avrogato:decode({<<"phoneproxy.sms_delivered">>, 4}, EncodedConfluent).
{ok,[{<<"data">>,
      [{<<"user_key_bytes">>,<<"123456">>},
       {<<"sequence_uuid">>,
        <<"123e4567-e89b-12d3-a456-426655440000">>},
       {<<"error_code">>,<<"30006">>},
       {<<"status">>,<<"failed">>}]},
     {<<"source">>,<<"phoneproxy">>},
     {<<"ts">>,1570040294933},
     {<<"uuid">>,<<"123e4567-e89b-12d3-a456-426655440000">>},
     {<<"version">>,1}]}
7> avrogato:decode({<<"phoneproxy.sms_delivered">>, 4}, EncodedOCF).
{ok,[{<<"data">>,
      [{<<"user_key_bytes">>,<<"123456">>},
       {<<"sequence_uuid">>,
        <<"123e4567-e89b-12d3-a456-426655440000">>},
       {<<"error_code">>,<<"30006">>},
       {<<"status">>,<<"failed">>}]},
     {<<"source">>,<<"phoneproxy">>},
     {<<"ts">>,1570040294933},
     {<<"uuid">>,<<"123e4567-e89b-12d3-a456-426655440000">>},
     {<<"version">>,1}]}

```

If you're trying to decode the wrong schema for what is actually encoded, an error value is returned. Do note that OCF as a format carries little version-related information and therefore will mostly warn about mismatching schema names more than just versions. Also note that while all keys and types are respected, the order of keys within the lists may change.

If you end up having to operate on a data format whose schema you do not know, you can use the `avrogato:decode_any(Record)` call to dynamically figure out what you get, regardless of the header format:

```
8> avrogato:decode_any(EncodedOCF).
{<<"phoneproxy.sms_delivered">>,
 [{<<"data">>,
   [{<<"user_key_bytes">>,<<"123456">>},
    {<<"sequence_uuid">>,
     <<"123e4567-e89b-12d3-a456-426655440000">>},
    {<<"error_code">>,<<"30006">>},
    {<<"status">>,<<"failed">>}]},
  {<<"source">>,<<"phoneproxy">>},
  {<<"ts">>,1570040294933},
  {<<"uuid">>,<<"123e4567-e89b-12d3-a456-426655440000">>},
  {<<"version">>,1}]}
```

Given that neither header format (OCF or Confluent) carries information about the schema version itself, the returned value is limited to be `{SchemaName, Payload}`.

Using in Elixir
-----

Add to your project as:

```elixir
defp deps do
    {:avrogato, github: "postmates/avrogato-erl", branch: "master", manager: :rebar3}
    ...
end
```

The Erlang API can then be used as-is.

Specify the following OTP configuration values before application start to impact its behaviour:

- `{:registry, 'http://example.org:8080'}`: Specify the registry from which to synchronize and fetch content. Without defining this value, the application will not boot. Supports HTTPS and HTTP Basic Auth.
- `{:prefetch, :all | ["namespace.schema", ...]}`: Specify which schemas to load on-start (blocking operation, best effort). We recommend fetching only specific sets of them rather than `all` since the full set can take minutes. The objective should be to pay a synchronization cost at boot time to ensure no blocking once the app starts accepting traffic. Defaults to `[]`

Schema Management
----

The schema is downloaded interactively as needed, and then cached in an ETS table for further access. An internal index is maintained, but if many schemas need to be fetched such that a snapshot of them is required for a quicker boot/start time, snapshotting function exists. To see about them, call `rebar3 edoc` on the repository and see the `avrogato_registry` module documentation.


Testing
-------

This library relies on PropEr and Common Test to run tests, but will also call Dialyzer, xref, and others. To run the full test battery, call:

    $ rebar3 check

TODO
----

- potentially support maps for encoding/decoding of terms? would require erlavro upgrades, probably.
