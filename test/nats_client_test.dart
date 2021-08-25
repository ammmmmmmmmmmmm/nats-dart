import 'dart:async';

import 'package:logging/logging.dart';
import 'package:nats/nats.dart';
import 'package:nats/src/protocol_handler.dart';
import 'package:nats/src/tcp_client.dart';
import 'package:test/test.dart';
import 'package:mockito/mockito.dart';
import 'dart:io';

void main() {
  const INFO_MSG =
      "INFO {\"server_id\":\"H2VckPfmIYJ4gMYnMUDLh9\",\"version\":\"1.3.0\",\"proto\":1,\"go\":\"go1.11\",\"host\":\"0.0.0.0\",\"port\":4222,\"max_payload\":1048576,\"client_id\":5,\"connect_urls\":[\"[fd3c:678c:d85b:400:189e:8a35:c372:8f71]:4222\",\"[fd3c:678c:d85b:400:f5c2:cdb0:7fee:9e36]:4222\",\"192.168.1.5:4222\",\"[fd3c:678c:d85b:400:189e:8a35:c372:8f71]:4333\",\"[fd3c:678c:d85b:400:f5c2:cdb0:7fee:9e36]:4333\",\"192.168.1.5:4333\"]}";

  const DATA_MSG = "MSG foo munukutla 3${CR_LF}Hey";

  final mockSocket = MockSocket();
  final mockTcpClient = MockTcpClient();
  late NatsClient client;

  setUp(() async {
    client = NatsClient("localhost", 4223, logLevel: Level.ALL);
  });

  test("Connect call succeeeds", () async {
    when(mockSocket.transform(any!)).thenAnswer(
        (_) => Stream<String>.fromIterable([INFO_MSG, DATA_MSG, PING]));
    when(mockTcpClient.connect()).thenAnswer((_) => Future.value(mockSocket));
    client.tcpClient = mockTcpClient;
    client.connect();
  });
}

class MockTcpClient extends Mock implements TcpClient {}

class MockSocket extends Mock implements Socket {}

class MockProtocolHandler extends Mock implements ProtocolHandler {}
