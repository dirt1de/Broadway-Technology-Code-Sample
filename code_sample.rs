use crate::db_access::RedisConnection;
use crate::text_chat_service::NewMessageInfo::{NewMessageStreamSender, NewMessageStruct};
use crate::text_chat_service::NewUserInfo::{NewUserStreamSender, NewUserStruct};
use crate::textchat::text_chat_server::TextChat;
use crate::textchat::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{error, info};
use uuid::Uuid;

type UsersStreamSender = mpsc::Sender<Result<UsersResponse, Status>>;
type MessagesStreamSender = mpsc::Sender<Result<MessageResponse, Status>>;
type RoomUsersStreamSenderMap = Arc<RwLock<HashMap<String, Vec<(String, UsersStreamSender)>>>>;
type RoomMessagesStreamSenderMap =
    Arc<RwLock<HashMap<String, Vec<(String, MessagesStreamSender)>>>>;

#[derive(Debug)]
pub enum NewUserInfo {
    NewUserStruct(String, User),
    NewUserStreamSender(String, String, UsersStreamSender),
}

#[derive(Debug)]
enum NewMessageInfo {
    NewMessageStruct(String, MessageResponse),
    NewMessageStreamSender(String, String, MessagesStreamSender),
}

pub struct TextChatService {
    // TODO: remove the RwLock over the connection
    redis_connection: Arc<RwLock<RedisConnection>>,
    new_user_info_tx: mpsc::Sender<NewUserInfo>,
    new_message_info_tx: mpsc::Sender<NewMessageInfo>,
}

impl TextChatService {
    pub fn new(redis_connection: Arc<RwLock<RedisConnection>>) -> TextChatService {
        let room_users_stream_map: RoomUsersStreamSenderMap =
            Arc::new(RwLock::new(HashMap::default()));
        let messages_stream_map: RoomMessagesStreamSenderMap =
            Arc::new(RwLock::new(HashMap::default()));

        let (new_user_info_tx, mut new_user_info_rx) = mpsc::channel::<NewUserInfo>(1000);
        let (new_message_info_tx, mut new_message_info_rx) = mpsc::channel::<NewMessageInfo>(1000);

        tokio::spawn(async move {
            info!("PubSub on new user started");
            let room_users_stream_map_clone = room_users_stream_map.clone();
            while let Some(new_user_info) = new_user_info_rx.recv().await {
                match new_user_info {
                    NewUserStruct(room_id, new_user) => {
                        // Monitor new user & broadcast to the RoomUsersStreamSenderMap
                        info!("Receiving new user in room: {}", room_id);

                        if let Err(user_id) = Self::broadcast_new_user_to_room(
                            room_id,
                            new_user,
                            room_users_stream_map_clone.clone(),
                        )
                        .await
                        {
                            info!("User with id {} is not online", user_id);
                            // This is for the case where user with user_id has dropped the rx side
                            let mut room_users_stream_map_guard =
                                room_users_stream_map_clone.write().await;
                            room_users_stream_map_guard.remove(&user_id);
                        }
                    }
                    NewUserStreamSender(room_id, user_id, new_user_sender) => {
                        info!("Receiving new user sender with user_id: {}", user_id);
                        let mut room_users_stream_map_guard =
                            room_users_stream_map_clone.write().await;
                        // Check if the room exists or not. If exists, then just update the values
                        // Else, it means this user is the first user in that room
                        if let Some(sender_wrapper) = room_users_stream_map_guard.get_mut(&room_id)
                        {
                            sender_wrapper.push((user_id, new_user_sender));
                            info!("Senders in room {} are {:#?}", room_id, sender_wrapper);
                        } else {
                            let room_id_clone = room_id.clone();
                            info!("Creating room_users_stream_map for room: {}", room_id_clone);
                            room_users_stream_map_guard
                                .insert(room_id, vec![(user_id, new_user_sender)]);
                            info!(
                                "Creating room_users_stream_map for room: {} finished",
                                room_id_clone
                            );
                        }
                    }
                }
            }
        });

        tokio::spawn(async move {
            info!("PubSub on new message started");
            let messages_stream_map_clone = messages_stream_map.clone();
            while let Some(new_message_info) = new_message_info_rx.recv().await {
                info!("Receive new message in pubsub on new message");
                match new_message_info {
                    NewMessageStruct(room_id, new_message) => {
                        // Monitor new message & broadcast to the RoomMessagesStreamSenderMap
                        info!("Receiving new message in room: {}", room_id);

                        if let Err(user_id) = Self::broadcast_new_message_to_room(
                            room_id,
                            new_message,
                            messages_stream_map_clone.clone(),
                        )
                        .await
                        {
                            // This is for the case where user with user_id has dropped the rx side
                            let mut messages_stream_map_guard =
                                messages_stream_map_clone.write().await;
                            messages_stream_map_guard.remove(&user_id);
                        }
                    }
                    NewMessageStreamSender(room_id, user_id, new_message_sender) => {
                        let mut messages_stream_map_guard = messages_stream_map_clone.write().await;
                        if let Some(sender_wrapper) = messages_stream_map_guard.get_mut(&room_id) {
                            sender_wrapper.push((user_id, new_message_sender))
                        } else {
                            messages_stream_map_guard
                                .insert(room_id, vec![(user_id, new_message_sender)]);
                        }
                    }
                }
            }
        });

        TextChatService {
            redis_connection,
            new_user_info_tx,
            new_message_info_tx,
        }
    }

    async fn broadcast_new_user_to_room(
        room_id: String,
        new_user: User,
        room_users_stream_map: RoomUsersStreamSenderMap,
    ) -> Result<(), String> {
        let room_user_stream_map_guard = room_users_stream_map.read().await;
        let senders = room_user_stream_map_guard.get(&room_id).unwrap();
        for (user_id, sender) in senders {
            let new_user_clone = new_user.clone();
            info!(
                "Broadcasting to user with id: {} in room with id: {}",
                user_id, room_id
            );
            if let Err(_err) = sender
                .send(Ok(UsersResponse {
                    user: Some(new_user_clone),
                }))
                .await
            {
                error!("Fail to broadcast new user to user with id: {}", user_id);
                return Err(user_id.to_string());
            }
        }

        Ok(())
    }

    async fn broadcast_new_message_to_room(
        room_id: String,
        new_message: MessageResponse,
        room_messages_stream_map: RoomMessagesStreamSenderMap,
    ) -> Result<(), String> {
        let room_messages_stream_map_guard = room_messages_stream_map.read().await;
        let senders = room_messages_stream_map_guard.get(&room_id).unwrap();

        info!(
            "All message sender in room: {} are: {:#?}",
            room_id, senders
        );
        for (user_id, sender) in senders {
            let new_message_clone = new_message.clone();
            if let Err(err) = sender.send(Ok(new_message_clone)).await {
                error!(
                    "Fail to broadcast new message to user with id: {} because: {}",
                    user_id, err
                );
                return Err(user_id.to_string());
            }
        }

        Ok(())
    }
}