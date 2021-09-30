using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.Json;
using System.Linq;
using System.IO;
using System.Reflection;
using System.Xml.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using ProtoBuf;
using StackExchange.Redis;

const int N = 50000;
//const int N = 50;

var redis = ConnectionMultiplexer.Connect("localhost");
var db = redis.GetDatabase();
var stopWatch = new Stopwatch();

var binaryDtoList = Enumerable.Range(1, N)
  .Select(i => new BinaryDto(
    "PropertyA" + i,
    "PropertyB" + i,
    "PropertyC" + i,
    Guid.NewGuid()))
   .ToList();

stopWatch.Start();
using (var binarySetStream = new MemoryStream())
{
  var binarySerializer = new BinaryFormatter();
  binarySerializer.Serialize(binarySetStream, binaryDtoList);

  db.StringSet(
    "binary-cache-key",
    binarySetStream.ToArray(),
    TimeSpan.FromMinutes(5));
}
stopWatch.Stop();

Console.WriteLine($"Binary write = {stopWatch.ElapsedMilliseconds} ms");

stopWatch.Restart();
var binaryCacheEntry = db.StringGet("binary-cache-key");

using (var binaryGetStream = new MemoryStream(binaryCacheEntry))
{
  var binaryDeserializer = new BinaryFormatter();
  binaryDeserializer.Deserialize(binaryGetStream);
}
stopWatch.Stop();

Console.WriteLine($"Binary read = {stopWatch.ElapsedMilliseconds} ms");
Console.WriteLine();

var xmlDtoList = Enumerable.Range(1, N)
  .Select(i => new XmlDto(
    "PropertyA" + i,
    "PropertyB" + i,
    "PropertyC" + i,
    Guid.NewGuid()))
   .ToList();

stopWatch.Start();
using (var xmlSetStream = new MemoryStream())
{
  var xmlSerializer = new XmlSerializer(xmlDtoList.GetType());
  xmlSerializer.Serialize(xmlSetStream, xmlDtoList);

  db.StringSet(
    "xml-cache-key",
    xmlSetStream.ToArray(),
    TimeSpan.FromMinutes(5));
}
stopWatch.Stop();

Console.WriteLine($"Xml write = {stopWatch.ElapsedMilliseconds} ms");

stopWatch.Restart();
var xmlCacheEntry = db.StringGet("xml-cache-key");

using (var xmlGetStream = new MemoryStream(xmlCacheEntry))
{
  var xmlDeserializer = new XmlSerializer(xmlDtoList.GetType());
  xmlDeserializer.Deserialize(xmlGetStream);
}
stopWatch.Stop();

Console.WriteLine($"Xml read = {stopWatch.ElapsedMilliseconds} ms");
Console.WriteLine();

var jsonDtoList = Enumerable.Range(1, N)
  .Select(i => new JsonDto(
    "PropertyA" + i,
    "PropertyB" + i,
    "PropertyC" + i,
    Guid.NewGuid()))
   .ToList();

stopWatch.Start();
var jsonSetStream = JsonSerializer.Serialize(jsonDtoList);

db.StringSet(
  "json-cache-key",
  jsonSetStream,
  TimeSpan.FromMinutes(5));
stopWatch.Stop();

Console.WriteLine($"Json write = {stopWatch.ElapsedMilliseconds} ms");

stopWatch.Restart();
var jsonCacheEntry = db.StringGet("json-cache-key");

JsonSerializer.Deserialize<List<JsonDto>>(jsonCacheEntry);
stopWatch.Stop();

Console.WriteLine($"Json read = {stopWatch.ElapsedMilliseconds} ms");
Console.WriteLine();

var protoDtoList = Enumerable.Range(1, N)
  .Select(i => new ProtoDto(
    "PropertyA" + i,
    "PropertyB" + i,
    "PropertyC" + i,
    Guid.NewGuid()))
   .ToList();

stopWatch.Start();
using (var protoSetStream = new MemoryStream())
{
  Serializer.Serialize(protoSetStream, protoDtoList);

  db.StringSet(
    "proto-cache-key",
    protoSetStream.ToArray(),
    TimeSpan.FromMinutes(5));
}
stopWatch.Stop();

Console.WriteLine($"Proto write = {stopWatch.ElapsedMilliseconds} ms");

stopWatch.Restart();
var protoCacheEntry = db.StringGet("proto-cache-key");

using (var protoGetStream = new MemoryStream(protoCacheEntry))
{
  Serializer.Deserialize<List<ProtoDto>>(protoGetStream);
}
stopWatch.Stop();

Console.WriteLine($"Proto read = {stopWatch.ElapsedMilliseconds} ms");
Console.WriteLine();

CacheDto(jsonDtoList);
CacheDto(protoDtoList);

void CacheDto<T>(T obj)
{
  var type = typeof(T);

  if (type.IsGenericType &&
    type.GenericTypeArguments[0]
      .GetCustomAttribute(
        typeof(ProtoContractAttribute),
        false) != null)
  {
    Console.WriteLine($"Use the ProtoBuf serializer = {obj}");
  }
  else
  {
    Console.WriteLine($"Use the JSON serializer = {obj}");
  }
}

[Serializable]
record BinaryDto(
  string propertyA,
  string propertyB,
  string propertyC,
  Guid id);

public record XmlDto(
  [property: XmlElementAttribute()]
  string propertyA,
  [property: XmlElementAttribute()]
  string propertyB,
  [property: XmlElementAttribute()]
  string propertyC,
  [property: XmlAttribute()]
  Guid id)
{
  XmlDto() : this(
    string.Empty,
    string.Empty,
    string.Empty,
    Guid.Empty) {}
};

record JsonDto(
  string propertyA,
  string propertyB,
  string propertyC,
  Guid id);
  
[ProtoContract(SkipConstructor = true)]
record ProtoDto(
  [property: ProtoMember(1)]
  string propertyA,
  [property: ProtoMember(2)]
  string propertyB,
  [property: ProtoMember(3)]
  string propertyC,
  [property: ProtoMember(4)]
  Guid id);
