import 'constants.dart';
import 'tcp_client.dart';
import 'server_info.dart';
import 'nats_message.dart';
import 'connection_options.dart';
import 'subscription.dart';
import 'protocol_handler.dart';

import 'dart:io';
import "dart:convert";
import 'dart:async';

import 'package:logging/logging.dart';

class NatsClient {
  String _currentHost;
  int _currentPort;

  String get currentHost => _currentHost;
  int get currentPort => _currentPort;

  Socket _socket;
  TcpClient tcpClient;
  ServerInfo _serverInfo;

  StreamController<NatsMessage> _messagesController;
  List<Subscription> _subscriptions;
  ProtocolHandler _protocolHandler;

  final Logger log = Logger("NatsClient");

  //message 不相等

  String state = "init";
  String _receviedMsg = "";

  NatsClient(String host, int port, {Level logLevel = Level.INFO}) {
    _currentHost = host;
    _currentPort = port;

    _serverInfo = ServerInfo();
    _subscriptions = List();
    _messagesController = new StreamController.broadcast();
    tcpClient = TcpClient(host: host, port: port);
    _initLogger(logLevel);
  }

  void _initLogger(Level level) {
    Logger.root.level = level;
    Logger.root.onRecord.listen((LogRecord rec) {
      print('${rec.level.name}: ${rec.time}: ${rec.message}');
    });
  }

  /// Connects to the given NATS url
  ///
  /// ```dart
  /// var client = NatsClient("localhost", 4222);
  /// var options = ConnectionOptions()
  /// options
  ///  ..verbose = true
  ///  ..pedantic = false
  ///  ..tlsRequired = false
  /// await client.connect(connectionOptions: options);
  /// ```
  void connect(
      {ConnectionOptions connectionOptions,
      void onConnect(),
      void onClusterupdate(ServerInfo info)}) async {
    _socket = await tcpClient.connect();
    if (onConnect != null) {
      onConnect();
    }
    _protocolHandler = ProtocolHandler(socket: _socket, log: log);
    
    utf8.decoder.bind(_socket).listen((data) {
        // print("socket_start: ${data.startsWith(MSG)}");
        if(!data.startsWith(MSG) && state != 'wait'){
          // print("socket_start condition: ${data}");
          data.split(CR_LF).forEach((f)=>_serverPushString(f,
          connectionOptions: connectionOptions,
          onClusterupdate: onClusterupdate));
        }else {
          // print("socket:${data.length}");
          _serverPushString(data,
          connectionOptions: connectionOptions,
          onClusterupdate: onClusterupdate);
        }
      
    }, onDone: () {
      log.info("Host down. Switching to next available host in cluster");
      _removeCurrentHostFromServerInfo(_currentHost, _currentPort);
      _reconnectToNextAvailableInCluster(
          opts: connectionOptions, onClusterupdate: onClusterupdate);
    });

  }

  Future destroy() async{

    await _socket.close();
    _socket.destroy();

  }

  void _removeCurrentHostFromServerInfo(String host, int port) =>
      _serverInfo.serverUrls.removeWhere((url) => url == "$host:$port");

  void _reconnectToNextAvailableInCluster(
      {ConnectionOptions opts, void onClusterupdate(ServerInfo info)}) async {
    var urls = _serverInfo.serverUrls;
    bool isIPv6Address(String url) => url.contains("[") && url.contains("]");
    for (var url in urls) {
      tcpClient = _createTcpClient(url, isIPv6Address);
      try {
        await connect(
            connectionOptions: opts, onClusterupdate: onClusterupdate);
        log.info("Successfully switched client to $url now");
        _carryOverSubscriptions();
        break;
      } catch (ex) {
        log.fine("Tried connecting to $url but failed. Moving on");
      }
    }
  }

  /// Returns a [TcpClient] from the given [url]
  TcpClient _createTcpClient(String url, bool checker(String url)) {
    log.fine("Trying to connect to $url now");
    int port = int.parse(url.split(":")[url.split(":").length - 1]);
    String host;
    if (checker(url)) {
      // IPv6 address
      host = url.substring(url.indexOf("[") + 1, url.indexOf("]"));
    } else {
      // IPv4 address
      host = url.substring(0, url.indexOf(":"));
    }
    return TcpClient(host: host, port: port);
  }

  /// Carries over [Subscription] objects from one host to another during cluster rearrangement
  void _carryOverSubscriptions() {
    _subscriptions.forEach((subscription) {
      _doSubscribe(true, subscription.subscriptionId, subscription.subject);
    });
  }

  void _serverPushString(String serverPushString,
      {ConnectionOptions connectionOptions,
      void onClusterupdate(ServerInfo info)}) {

    if (serverPushString.startsWith(INFO)) {
      _setServerInfo(serverPushString.replaceFirst(INFO, ""),
          connectionOptions: connectionOptions);
      // If it is a new serverinfo packet, then call the update handler
      if (onClusterupdate != null) {
        onClusterupdate(_serverInfo);
      }
    }  else if (serverPushString.startsWith(PING)) {
      _protocolHandler.sendPong();
    } else if (serverPushString.startsWith(OK)) {
      log.fine("Received server OK");
    }else  if(serverPushString.startsWith(MSG) || state == 'wait'){

            // print('revice:${serverPushString.length}');
            //解决残缺数据
            if(state == 'wait'  && serverPushString.split(CR_LF).length > 1) {
              // print('解决粘包 : ${serverPushString.split(CR_LF).length}  ${serverPushString.split(CR_LF)[1]}');
              serverPushString.split(CR_LF).forEach((m)=>_serverPushString(m));
            }else {

            serverPushString = _receviedMsg + serverPushString;
            var msgArr = serverPushString.split(CR_LF);
            var header = msgArr[0];
            var payload = msgArr.length > 1 ?  msgArr[1] : ""; //接收到的payload长度

            int bytsLength = int.parse(header.split(" ").last);  

             if (bytsLength == payload.length) {
              state = "init";
              _receviedMsg = "";
            _convertToMessages(serverPushString)
           .forEach((msg) => _messagesController.add(msg));

          }else {
            this.state = 'wait';
            var msgArr = serverPushString.split(CR_LF);
            var header = msgArr[0];
            var payload = msgArr.length > 1 ?  msgArr[1] : ""; 
            _receviedMsg =  '$header$CR_LF$payload';    
          } 
        }
      // _convertToMessages(serverPushString)
      //     .forEach((msg) => _messagesController.add(msg));
    }
  }

  void _setServerInfo(String serverInfoString,
      {ConnectionOptions connectionOptions}) {
    try {
      Map<String, dynamic> map = jsonDecode(serverInfoString);
      _serverInfo.serverId = map["server_id"];
      _serverInfo.version = map["version"];
      _serverInfo.protocolVersion = map["proto"];
      _serverInfo.goVersion = map["go"];
      _serverInfo.host = map["host"];
      _serverInfo.port = map["port"];
      _serverInfo.maxPayload = map["max_payload"];
      _serverInfo.clientId = map["client_id"];
      try {
        if (map["connect_urls"] != null) {
          _serverInfo.serverUrls = map["connect_urls"].cast<String>();
        }
      } catch (e) {
        log.severe(e.toString());
      }
      if (connectionOptions != null) {
        _sendConnectionPacket(connectionOptions);
      }
    } catch (ex) {
      log.severe(ex.toString());
    }
  }

  void _sendConnectionPacket(ConnectionOptions opts) {
    _protocolHandler.connect(opts: opts);
    _protocolHandler.sendPing();
  }

  /// Publishes the [message] to the [subject] with an optional [replyTo] set to receive the response
  /// ```dart
  /// var client = NatsClient("localhost", 4222);
  /// await client.connect();
  /// client.publish("Hello World", "foo-topic");
  /// client.publish("Hello World", "foo-topic", replyTo: "reply-topic");
  /// ```
  void publish(String message, String subject,
      {void onSuccess(), void onError(Exception ex), String replyTo}) {
    if (_socket == null) {
      throw Exception(
          "Socket not ready. Please check if NatsClient.connect() is called");
    }
    _protocolHandler.pubish(message, subject, () {
      onSuccess();
    }, (err) {
      onError(err);
    }, replyTo: replyTo);
  }

  NatsMessage _convertToMessage(String serverPushString) {
    var message = NatsMessage();
    List<String> lines = serverPushString.split(CR_LF);
    List<String> firstLineParts = lines[0].split(" ");

    message.subject = firstLineParts[0];
    message.sid = firstLineParts[1];

    bool replySubjectPresent = firstLineParts.length == 4;

    if (replySubjectPresent) {
      message.replyTo = firstLineParts[2];
      message.length = int.parse(firstLineParts[3]);
    } else {
      message.length = int.parse(firstLineParts[2]);
    }

    message.payload = lines[1];
    return message;
  }

  List<NatsMessage> _convertToMessages(String serverPushString) =>
      serverPushString
          .split(MSG)
          .where((msg) => msg.length > 0)
          .map((msg) => _convertToMessage(msg))
          .toList();

  /// Subscribes to the [subject] with a given [subscriberId] and an optional [queueGroup] set to group the responses
  /// ```dart
  /// var client = NatsClient("localhost", 4222);
  /// await client.connect();
  /// var messageStream = client.subscribe("sub-1", "foo-topic"); // No [queueGroup] set
  /// var messageStream = client.subscribe("sub-1", "foo-topic", queueGroup: "group-1")
  ///
  /// messageStream.listen((message) {
  ///   // Do something awesome
  /// });
  /// ```
  Stream<NatsMessage> subscribe(String subscriberId, String subject,
      {String queueGroup}) {
    return _doSubscribe(false, subscriberId, subject);
  }

  Stream<NatsMessage> _doSubscribe(
      bool isReconnect, String subscriberId, String subject,
      {String queueGroup}) {
    if (_socket == null) {
      throw Exception(
          "Socket not ready. Please check if NatsClient.connect() is called");
    }

    _protocolHandler.subscribe(subscriberId, subject, () {
      if (!isReconnect) {
        _subscriptions.add(Subscription()
          ..subscriptionId = subscriberId
          ..subject = subject
          ..queueGroup = queueGroup);
      } else {
        log.fine("Carrying over subscription [${subject}]");
      }
    }, (err) {}, queueGroup: queueGroup);

    return _messagesController.stream
        .where((incomingMsg) => matchesRegex(subject, incomingMsg.subject));
  }

  bool matchesRegex(String listeningSubject, String incomingSubject) {
    var expression = RegExp("$listeningSubject");
    return expression.hasMatch(incomingSubject);
  }

  void unsubscribe(String subscriberId, {int waitUntilMessageCount}) {
    if (_socket == null) {
      throw Exception(
          "Socket not ready. Please check if NatsClient.connect() is called");
    }

    _protocolHandler.unsubscribe(subscriberId, waitUntilMessageCount, () {
      _subscriptions.removeWhere(
          (subscription) => subscription.subscriptionId == subscriberId);
    }, (ex) {});
  }
}
