# lala

package io.files;

import java.net.InetAddress;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

import interfaces.visitors.FileMessageVisitor;
import interfaces.visitors.LoggerVisitor;
import io.files.filePartition.FileAssembler;
import io.files.filePartition.FileDisassembler;
import messages.ThreadMessage;
import messages.internal.InternalMessage;
import messages.internal.received.InternalReceivedChunkMessage;
import messages.internal.received.InternalReceivedEndMessage;
import messages.internal.received.InternalReceivedFileMessage;
import messages.internal.received.InternalReceivedIdMessage;
import messages.internal.received.InternalReceivedMessage;
import messages.internal.received.InternalReceivedTalkMessage;
import messages.internal.requested.send.InternalRequestSendFileMessage;
import messages.internal.requested.send.InternalRequestSendFullFileMessage;
import messages.internal.requested.send.InternalRequestSendMessage;
import messages.internal.requested.send.InternalRequestSendTalkMessage;
import utils.ConsoleLogger;
import utils.Constants;
import utils.Exceptions.FileException;

public class FileManager implements FileMessageVisitor, LoggerVisitor {
    private FileLogger logger;
    private HashMap<InetAddress, FileAssembler> receivingFiles;   // ip -> File
    private HashMap<InetAddress, FileDisassembler> sendingfiles;  // ip -> File
    private BlockingQueue<InternalMessage> managerSenderQueue;

    public FileManager(BlockingQueue<InternalMessage> managerSenderQueue) {
        this.managerSenderQueue = managerSenderQueue;

        logger         = new FileLogger();
        receivingFiles = new HashMap<InetAddress, FileAssembler>();
        sendingfiles   = new HashMap<InetAddress, FileDisassembler>();
    }

    private void sendAck(InternalReceivedIdMessage message) {
        managerSenderQueue.offer(
            ThreadMessage.internalMessage(getClass())
                .request()
                .send()
                .ack(message.getMessageId())
                .to(message.getSourceIp())
                .at(message.getPort())
        );
    }

    private void sendNAck(InternalReceivedIdMessage message, String reason) {
        managerSenderQueue.offer(
            ThreadMessage.internalMessage(getClass())
                .request()
                .send()
                .nAck(message.getMessageId())
                .because(reason)
                .to(message.getSourceIp())
                .at(message.getPort())
        );

    }
    
    // ****************************************************************************************************
    // Visitor pattern for InternalMessage

    // **************************************************
    // File management

    @Override
    public void visit(InternalReceivedFileMessage message) {
        String fileName;
        long fileSize;
        FileAssembler fileAssembler;
        String nAckReason;
        
        logger.logReceived(message);

        fileName = message.getFileName();
        fileSize = message.getFileSize();

        try {
            fileAssembler = FileAssembler.of(fileName, fileSize);
        } catch (NoSuchAlgorithmException e) {
            ConsoleLogger.logError(e);
            nAckReason = "Error creating file assembler";
            sendNAck(message, nAckReason);
            return;
        }
        
        if (receivingFiles.containsKey(message.getSourceIp())) {
            nAckReason = "Already receiving a file from that ip";
            sendNAck(message, nAckReason);
            return;
        }

        if (fileAssembler == null) {
            nAckReason = "Invalid FileName or FileSize";
            sendNAck(message, nAckReason);
            return;
        }

        receivingFiles.put(message.getSourceIp(), fileAssembler);
        sendAck(message);
    }

    @Override
    public void visit(InternalReceivedChunkMessage message) {
        ConsoleLogger.logBlue("1");
        int sequenceNumber;
        byte[] chunkData;
        InetAddress sourceIp;
        FileAssembler fileAssembler;
        String nAckReason;

        logger.logReceived(message);

        sequenceNumber = message.getSequenceNumber();
        chunkData      = message.getData();
        sourceIp       = message.getSourceIp();
        fileAssembler  = receivingFiles.get(sourceIp);

        if (fileAssembler == null) {
            ConsoleLogger.logBlue("2");
            nAckReason = "Not receiving a file from that ip";
            sendNAck(message, nAckReason);
            return;
        }
        if (!fileAssembler.addPacket(sequenceNumber, chunkData)) {
            if (fileAssembler.getErrorMessage() == null) {
                // Out of order packet, wait for timeout to resend
                return;}

            logger.logInternal(
                Constants.Strings.DISCARTED_CHUNK_FORMAT.formatted(
                    sequenceNumber, chunkData.length, fileAssembler.getErrorMessage()
                )
            );
            nAckReason = fileAssembler.getErrorMessage();
            sendNAck(message, nAckReason);
            return;
        }

        ConsoleLogger.logBlue("5");
        sendAck(message);
    }

    @Override
    public void visit(InternalReceivedEndMessage message) {
        String receivedHash;
        InetAddress sourceIp;
        FileAssembler fileAssembler;
        String nAckReason;

        logger.logReceived(message);

        receivedHash  = message.getFileHash();
        sourceIp      = message.getSourceIp();
        fileAssembler = receivingFiles.get(sourceIp);

        if (fileAssembler == null) {
            nAckReason = "Not receiving a file from that ip";
            sendNAck(message, nAckReason);
            return;
        }
        
        if (!fileAssembler.completeCreation(receivedHash)) {
            if (fileAssembler.getErrorMessage() == null)
                // Out of order packet, wait for timeout to resend
                return;

            nAckReason = fileAssembler.getErrorMessage();
            sendNAck(message, nAckReason);
        }

        if (!fileAssembler.isComplete()) return;

        receivingFiles.remove(sourceIp);
        
        if (fileAssembler.getErrorMessage() == null) {
            sendAck(message);
        } else {
            nAckReason = fileAssembler.getErrorMessage();
            sendNAck(message, nAckReason);
        }
    }

    @Override
    public void visit(InternalRequestSendFileMessage message) {
        FileDisassembler fileDisassembler;

        logger.logSent(message);
        try {
            fileDisassembler = FileDisassembler.of(message.getFileName());
        } catch (NoSuchAlgorithmException e) {
            throw new FileException("No such algorithm");
        }

        if (fileDisassembler == null) {
            throw new FileException("Encountered a problem when creating the file disassembler");
        }

        message.setFileSize(fileDisassembler.getFileSize());
        sendingfiles.put(message.getDestinationIp(), fileDisassembler);
    }


    @Override
    public void visit(InternalRequestSendFullFileMessage message) {
        FileDisassembler fileDisassembler;
        byte[] chunkData;
        int sequenceNumber;
        InetAddress destinationIp;
        int port;

        logger.logInternal(message);
        fileDisassembler = sendingfiles.get(message.getDestinationIp());

        if (fileDisassembler == null) return;

        destinationIp  = message.getDestinationIp();
        port           = message.getPort();
        sequenceNumber = 0;
        while((chunkData = fileDisassembler.readChunk()) != null) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                ConsoleLogger.logError(e);
            }
            managerSenderQueue.offer(
                ThreadMessage.internalMessage(getClass())
                    .request()
                    .send()
                    .chunk(sequenceNumber++)
                    .data(chunkData)
                    .to(destinationIp)
                    .at(port)
            );
        }

        managerSenderQueue.offer(
            ThreadMessage.internalMessage(getClass())
                .request()
                .send()
                .end(fileDisassembler.computeHash())
                .to(destinationIp)
                .at(port)
        );
    }

    // **************************************************
    // log and ack

    @Override
    public void visit(InternalReceivedTalkMessage message) {
        logger.logReceivedTalk(message);
        sendAck(message);
    }

    // **************************************************
    // log only

    @Override
    public void visit(InternalReceivedMessage message) {
        logger.logReceived(message);
    }

    @Override
    public void visit(InternalMessage message) {
        logger.logInternal(message);
    }

    @Override
    public void visit(InternalRequestSendMessage message) {
        logger.logSent(message);
    }

    @Override
    public void visit(InternalRequestSendTalkMessage message) {
        logger.logSentTalk(message);
    }
}
