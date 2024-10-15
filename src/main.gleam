import gleam/io

import gleam/bit_array
import gleam/bytes_builder as bb
import gleam/erlang/process
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/string
import glisten
import gluid

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
    _ -> -1
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
    35 -> DescribeTopicPartitions
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
      body:bits,
    >> ->
      Ok(Request(HeaderV1(req_len, api_key, api_version, correlation_id), body))
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
      client_id:size(client_id_length),
      body:bits,
    >> ->
      case bit_array.to_string(<<client_id>>) {
        Ok(client_id) ->
          Ok(Request(
            HeaderV2(req_len, api_key, api_version, correlation_id, client_id),
            body,
          ))
        Error(_) -> Error(CorruptMessage)
      }
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
    index: Int,
    leader_id: Int,
    leader_epoch: Int,
    replica_nodes: Int,
    isr_node: Int,
    eligibile_leader_replicas: Int,
    last_known_elr: Int,
    offline_replicas: Int,
  )
}

type Topic {
  Topic(
    name: String,
    topic_id: Int,
    is_intenrnal: Bool,
    partitions: List(Partition),
  )
}

// See: https://kafka.apache.org/protocol.html#protocol_types
type DataField {
  Kint(value: Int, size: Int, zigzag: Bool)
  //  Kuuid(value: gluid.BitArray)
  Kstring(value: Option(String), compact: Bool, nullable: Bool)
}

fn serialize_data_field(d: DataField) -> BitArray {
  case d {
    Kint(value, size, False) -> <<value:big-size(size)>>
    Kint(value, _, True) -> zigzag_encode(value)
    // STRING 
    Kstring(value, False, False) -> {
      bb.to_bit_array(
        bb.prepend(bb.from_string(option.unwrap(value, "")), <<
          string.byte_size(value):big-size(16),
        >>),
      )
    }
    // COMPACT_STRING 
    Kstring(value, False, True) -> {
      let value = option.unwrap(value, "")
      bb.to_bit_array(bb.prepend(
        bb.from_string(value),
        zigzag_encode(string.byte_size(value)),
      ))
    }
    // NULLABLE_STRING 
    Kstring(value, True, False) -> {
      case value {
        Some(value) -> {
          bb.to_bit_array(
            bb.prepend(bb.from_string(value), <<
              string.byte_size(value):big-size(16),
            >>),
          )
        }
        None -> <<-1:big-size(16)>>
      }
    }
    // COMPACT_NULLABLE_STRING 
    Kstring(value, True, True) -> todo
  }
}

fn zigzag_encode(i: Int) -> BitArray {
  todo
}

fn serialize_list(l: List(DataField)) -> BitArray {
  // length is + 1  
  let length = list.length(l) + 1

  let acc = bb.from_bit_array(<<length:big-size(32)>>)

  let result = list.fold(l, acc, append_data)

  bb.to_bit_array(result)
}

fn append_data(acc: bb.BytesBuilder, d: DataField) -> bb.BytesBuilder {
  bb.append(acc, <<d.data:big-size(d.size)>>)
}

fn describe_topic_partitions(
  req: Request,
  response: bb.BytesBuilder,
) -> Response {
  let topics: List(Topic) = [
    Topic("topic one", 101, False, []),
    Topic("topic 2", 102, False, []),
  ]

  let error_code = case get_api_version(req.header) {
    0 -> NoError
    _ -> UnsupportedVersion
  }

  let serialize_partition = fn(p: Partition) -> BitArray {
    // key, min version, max version, tag buffer
    <<
      api_error_code(error_code):big-size(16),
      p.index:big-size(32),
      p.leader_id:big-size(32),
      p.leader_epoch:big-size(32),
      p.replica_nodes:big-size(32),
      p.isr_node:big-size(32),
      p.eligibile_leader_replicas:big-size(32),
      p.last_known_elr:big-size(32),
      p.offline_replicas:big-size(32),
      0,
    >>
  }

  Response(
    bb.append_builder(
      response,
      bb.concat_bit_arrays([
        // error code 
        <<api_error_code(error_code):big-size(32)>>,
        // throttle time 
        <<0:big-size(32)>>,
        // topics 
        serialize_list(topics),
        // next_cursor 
        <<0, 0:big-size(32)>>,
      ]),
    ),
    error_code,
  )
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
  Response(
    bb.append(response, <<api_error_code(UnsupportedEndpointType):big-size(32)>>),
    UnsupportedEndpointType,
  )
}

pub fn main() {
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

      let response: Response = case to_api_key(api_key) {
        ApiVersions -> api_versions(req, body)
        DescribeTopicPartitions -> describe_topic_partitions(req, body)
        _ -> unknown_api_key_handler(req, body)
      }

      let body = bb.prepend(body, <<bb.byte_size(response.body):big-size(32)>>)

      let assert Ok(_) = glisten.send(conn, body)

      actor.continue(state)
    })
    |> glisten.serve(9092)

  process.sleep_forever()
}
