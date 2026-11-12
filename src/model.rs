use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Actor {
    User,
    Mcp,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthMethod {
    #[default]
    Password,
    PrivateKey,
    KeyboardInteractive,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Connection {
    pub id: String,
    pub name: String,
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_username")]
    pub username: String,
    #[serde(default)]
    pub group: String,
    #[serde(default)]
    pub auth: AuthMethod,
    #[serde(default)]
    pub private_key_path: Option<String>,
    #[serde(default)]
    pub mcp_allowed: bool,
    // 经过的代理 (代理管理中的 ID), 空为直连.
    #[serde(default)]
    pub proxy_id: Option<String>,
    // 终端中 sudo/su 等提示输入的管理员密码; 单独设置时密码加密保存在连接文件中.
    #[serde(default)]
    pub admin_password: AdminPassword,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AdminPassword {
    #[default]
    None,
    Login,
    Custom,
}

fn default_port() -> u16 {
    22
}

fn default_username() -> String {
    "root".into()
}

impl Connection {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.id.is_empty() && self.id.len() <= 128, "连接 ID 无效");
        anyhow::ensure!(
            !self.name.trim().is_empty() && self.name.len() <= 256,
            "连接名称无效"
        );
        anyhow::ensure!(
            !self.host.is_empty()
                && self.host.len() <= 255
                && !self
                    .host
                    .chars()
                    .any(|c| c.is_control() || c.is_whitespace()),
            "主机地址无效"
        );
        anyhow::ensure!(self.port != 0, "端口不能为 0");
        anyhow::ensure!(
            !self.username.is_empty()
                && self.username.len() <= 256
                && !self.username.chars().any(char::is_control),
            "用户名无效"
        );
        anyhow::ensure!(self.group.len() <= 256, "分组名称过长");
        Ok(())
    }
}
