import 'dart:io';
import 'constants.dart';
import 'connection_options.dart';
import 'package:logging/logging.dart';

class ProtocolHandler {
  final Socket? socket;
  final Logger? log;

  ProtocolHandler({this.socket, this.log});

  void connect({required ConnectionOptions opts}) {
    var messageBuffer = CONNECT +
        "{\"verbose\":${opts.verbose},\"pedantic\":${opts.pedantic},\"tls_required\":${opts.tlsRequired},\"name\":${opts.name},\"lang\":${opts.language},\"version\":${opts.version},\"protocol\":${opts.protocol}, \"user\":${opts.userName}, \"pass\":${opts.password}}" +
        CR_LF;
    socket!.write(messageBuffer);
  }

  void pubish(
      String message, String subject, void onDone(), void onError(Exception ex),
      {String? replyTo}) {
    String messageBuffer;

    int length = message.length;

    if (replyTo != null) {
      messageBuffer = "$PUB $subject $replyTo $length $CR_LF$message$CR_LF";
    } else {
      messageBuffer = "$PUB $subject $length $CR_LF$message$CR_LF";
    }
    try {
      socket!.write(messageBuffer);
      onDone();
    } catch (ex) {
      log!.severe(ex);
    }
  }

  void subscribe(String? subscriberId, String? subject, void onDone(),
      void onError(Exception ex),
      {String? queueGroup}) {
    String messageBuffer;

    if (queueGroup != null) {
      messageBuffer = "$SUB $subject $queueGroup $subscriberId$CR_LF";
    } else {
      messageBuffer = "$SUB $subject $subscriberId$CR_LF";
    }

    try {
      socket!.write(messageBuffer);
      onDone();
    } on Exception catch (ex) {
      log!.severe("Error while creating subscription $ex");
      onError(ex);
    } catch (ex) {
      onError(SocketException(ex.toString()));
    }
  }

  void unsubscribe(String subscriberId, int? waitUntilMessageCount,
      void onDone(), void onError(Exception ex)) {
    String messageBuffer;

    if (waitUntilMessageCount != null) {
      messageBuffer = "$UNSUB $subscriberId $waitUntilMessageCount$CR_LF";
    } else {
      messageBuffer = "$UNSUB $subscriberId$CR_LF";
    }

    try {
      socket!.write(messageBuffer);
      onDone();
    } on Exception catch (ex) {
      log!.severe("Error while unsubscribing $subscriberId");
      onError(ex);
    } catch (ex) {
      onError(SocketException(ex.toString()));
    }
  }

  void sendPong() {
    socket!.write("$PONG$CR_LF");
  }

  void sendPing() {
    socket!.write("$PING$CR_LF");
  }
}
