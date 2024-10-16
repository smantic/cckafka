import gleam/bit_array
import gleam/bytes_builder as bb
import gleam/erlang/process
import gleam/io
import gleam/list
import gleam/option.{None, Some}
import gleam/otp/actor
import gleam/result
import glisten
import impl/datafield.{type DataField, Kboolean, Kint, Kstring, Kuuid}

pub type ApiError {
  UnknownServerError
  NoError
  InvalidRequest
  CorruptMessage
  UnsupportedVersion
  UnknownTopicOrPartition
  RequestTimedOut
  UnsupportedEndpointType
}

fn api_error_code(code: ApiError) -> Int {
  // see: https://kafka.apache.org/protocol.html#protocol_error_codes 
  case code {
    UnknownServerError -> -1
    NoError -> 0
    CorruptMessage -> 2
    UnknownTopicOrPartition -> 3
    RequestTimedOut -> 7
    UnsupportedVersion -> 35
    InvalidRequest -> 42
    UnsupportedEndpointType -> 115
  }
}

pub type ApiKey {
  UnknownApiKey
  ApiVersions
  DescribeTopicPartitions
}

fn to_api_key(i: Int) -> ApiKey {
  case i {
    18 -> ApiVersions
    75 -> DescribeTopicPartitions
    _ -> UnknownApiKey
  }
}

pub type Header {
  HeaderV1(req_len: Int, api_key: Int, api_version: Int, correlation_id: Int)
  HeaderV2(
    req_len: Int,
    api_key: Int,
    api_version: Int,
    correlation_id: Int,
    client_id: String,
  )
}

pub type Request {
  Request(header: Header, body: BitArray)
}

pub fn decode_header(d: BitArray) -> Result(Request, ApiError) {
  case d {
    <<
      req_len:size(32),
      api_key:size(16),
      api_version:size(16),
      correlation_id:size(32),
      -1:size(16),
      body:bits,
    >> ->
      Ok(Request(
        HeaderV2(req_len, api_key, api_version, correlation_id, ""),
        body,
      ))
    <<
      req_len:size(32),
      api_key:size(16),
      api_version:size(16),
      correlation_id:size(32),
      client_id_length:size(16),
      // tag buffer? 
      rest:bits,
    >> -> {
      let size = client_id_length * 8
      let assert <<client_id_bits:size(size), rest:bits>> = rest
      case bit_array.to_string(<<client_id_bits:size(size)>>) {
        Ok(client_id) ->
          Ok(Request(
            HeaderV2(req_len, api_key, api_version, correlation_id, client_id),
            rest,
          ))
        Error(_) -> Error(CorruptMessage)
      }
    }
    <<
      req_len:size(32),
      api_key:size(16),
      api_version:size(16),
      correlation_id:size(32),
      body:bits,
    >> ->
      Ok(Request(HeaderV1(req_len, api_key, api_version, correlation_id), body))
    _ -> Error(CorruptMessage)
  }
}

fn get_api_version(h: Header) -> Int {
  case h {
    HeaderV1(_, _, api_version, _) -> api_version
    HeaderV2(_, _, api_version, _, _) -> api_version
  }
}

type Response {
  Response(body: bb.BytesBuilder, error_code: ApiError)
}

type Partition {
  Partition(
    error_code: DataField,
    index: DataField,
    leader_id: DataField,
    leader_epoch: DataField,
    replica_nodes: List(DataField),
    isr_node: List(DataField),
    eligibile_leader_replicas: List(DataField),
    last_known_elr: List(DataField),
    offline_replicas: List(DataField),
  )
}

type Topic {
  Topic(
    name: DataField,
    topic_id: DataField,
    is_internal: DataField,
    partitions: List(DataField),
  )
}

type DescribeTopicPartitionResponse {
  DescribeTopicParitionResponse(
    throttle_time: DataField,
    topics: List(Topic),
    topic_authorized_operations: DataField,
    topic_name: DataField,
    partition_index: DataField,
    tags: DataField,
  )
}

fn describe_topic_partitions(
  req: Request,
  response: bb.BytesBuilder,
) -> Response {
  let error_code = case get_api_version(req.header) {
    0 -> NoError
    _ -> UnsupportedVersion
  }
  io.debug(req)
  let assert <<array_len:size(16), rest:bits>> = req.body
  let #(topic_names, _rest) = decode_strings([], array_len, rest)

  // the rest of the request when we need it. 
  // let assert <<_partition_limit:size(32), rest:bits>> = rest
  // let assert #(_cursor, rest) = decode_compact_string(rest)
  // let assert <<partition_index:size(32)>> = rest  

  let topics = list.map(topic_names, get_topic)

  let r =
    DescribeTopicParitionResponse(
      throttle_time: datafield.Kint(100, 32, False),
      topics: topics,
      topic_authorized_operations: datafield.Kint(3576, 32, False),
      topic_name: datafield.Kstring(
        value: Some("topic name"),
        compact: True,
        nullable: False,
      ),
      partition_index: datafield.Kint(0, 32, False),
      tags: datafield.TaggedFields,
    )
  let topics =
    list.fold(topics, [], fn(acc: List(DataField), t: Topic) -> List(DataField) {
      io.debug(t.name)
      list.append(acc, [
        datafield.Kint(api_error_code(UnknownTopicOrPartition), 16, False),
        t.name,
        t.topic_id,
        t.is_internal,
        datafield.Kint(value: 1, size: 32, zigzag: False),
      ])
    })

  io.debug(r)

  Response(
    bb.append_builder(
      response,
      bb.concat_bit_arrays([
        datafield.serialize_data_field(r.throttle_time),
        datafield.serialize_list(topics),
        datafield.serialize_data_field(r.topic_authorized_operations),
        datafield.serialize_data_field(r.topic_name),
        datafield.serialize_data_field(r.partition_index),
        datafield.serialize_data_field(datafield.TaggedFields),
      ]),
    ),
    error_code,
  )
}

fn get_topic(topic_name: String) -> Topic {
  Topic(Kstring(Some(topic_name), True, True), Kuuid(""), Kboolean(True), [])
}

fn decode_strings(
  acc: List(String),
  num_elements: Int,
  bits: BitArray,
) -> #(List(String), BitArray) {
  case num_elements > 0 {
    True -> {
      let #(elm, rest) = decode_compact_string(bits)
      case elm {
        Ok(str) ->
          decode_strings(list.append(acc, [str]), num_elements - 1, rest)
        Error(_) -> panic
      }
    }
    False -> #(acc, bits)
  }
}

fn decode_compact_string(d: BitArray) -> #(Result(String, Nil), BitArray) {
  let #(len, rest) = datafield.zigzag_decode(d)
  let len = len * 8

  let assert <<slice:size(len), 0:size(8), rest:bits>> = rest
  #(bit_array.to_string(<<slice:size(len)>>), rest)
}

fn api_versions(req: Request, response: bb.BytesBuilder) -> Response {
  let error_code = case get_api_version(req.header) {
    0 | 1 | 2 | 3 | 4 -> NoError
    _ -> UnsupportedVersion
  }

  let serialize = fn(data: ApiKeySupport) -> BitArray {
    // key, min version, max version, tag buffer
    <<data.key:big-size(16), data.min:big-size(16), data.max:big-size(16), 0>>
  }

  Response(
    bb.append_builder(
      response,
      bb.concat_bit_arrays([
        // error code 
        <<api_error_code(error_code):big-size(32)>>,
        // arr length + 1
        <<3:big>>,
        serialize(ApiKeySupport(18, 0, 4)),
        serialize(ApiKeySupport(75, 0, 4)),
        // thottle time 
        <<0:size(32)>>,
        // tag buffer 
        <<0>>,
      ]),
    ),
    error_code,
  )
}

type ApiKeySupport {
  ApiKeySupport(key: Int, min: Int, max: Int)
}

fn unknown_api_key_handler(_r: Request, response: bb.BytesBuilder) -> Response {
  io.debug("unkown api key")
  Response(
    bb.append(response, <<api_error_code(UnsupportedEndpointType):big-size(32)>>),
    UnsupportedEndpointType,
  )
}

pub fn main() {
  io.println("running...")
  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(Nil, None) }, fn(msg, state, conn) {
      io.println("Received message!")

      let assert glisten.Packet(req_bits) = msg

      let req: Request = case decode_header(req_bits) {
        Ok(r) -> r
        Error(_e) -> panic
      }

      // its not well defined when V1 headers are used vs V2 Headers. Lets handle both. 
      let #(_req_len, api_key, _api_version, correlation_id, _client_id) = case
        req.header
      {
        HeaderV1(l, k, v, c) -> #(l, k, v, c, None)
        HeaderV2(l, k, v, c, cid) -> #(l, k, v, c, Some(cid))
      }

      let body = bb.from_bit_array(<<correlation_id:big-size(32)>>)

      io.debug(api_key)

      let response: Response = case to_api_key(api_key) {
        ApiVersions -> api_versions(req, body)
        DescribeTopicPartitions -> describe_topic_partitions(req, body)
        _ -> unknown_api_key_handler(req, body)
      }

      let body = bb.prepend(body, <<bb.byte_size(response.body):big-size(32)>>)

      io.debug(bit_array.base16_encode(bb.to_bit_array(body)))

      let assert Ok(_) = glisten.send(conn, body)

      actor.continue(state)
    })
    |> glisten.serve(9092)

  process.sleep_forever()
}
