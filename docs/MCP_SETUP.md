# Настройка Sequential Thinking MCP в Cursor

## Что такое Sequential Thinking MCP?

Sequential Thinking MCP — это сервер Model Context Protocol, который помогает AI-ассистенту думать последовательно и структурированно при решении сложных задач.

## Установка

### Вариант 1: Глобальная настройка (рекомендуется)

1. **Создайте или откройте файл:**
   ```
   ~/.cursor/mcp.json
   ```
   
   На Windows:
   ```
   C:\Users\ВАШ_ПОЛЬЗОВАТЕЛЬ\.cursor\mcp.json
   ```

2. **Добавьте конфигурацию:**
   ```json
   {
     "mcpServers": {
       "sequential-thinking": {
         "command": "npx",
         "args": [
           "-y",
           "@modelcontextprotocol/server-sequential-thinking"
         ]
       }
     }
   }
   ```

3. **Сохраните файл и перезапустите Cursor**

### Вариант 2: Локальная настройка (для проекта)

1. **Создайте файл в корне проекта:**
   ```
   .cursor/mcp.json
   ```

2. **Добавьте ту же конфигурацию** (см. выше)

3. **Перезапустите Cursor**

## Проверка установки

После перезапуска Cursor:

1. Откройте **Composer** (Ctrl+I или Cmd+I)
2. Попробуйте запрос, требующий последовательного мышления
3. AI должен использовать Sequential Thinking автоматически

## Альтернатива: Docker

Если у вас установлен Docker, можно использовать:

```json
{
  "mcpServers": {
    "sequential-thinking": {
      "command": "docker",
      "args": [
        "run",
        "--rm",
        "-i",
        "mcp/sequentialthinking"
      ]
    }
  }
}
```

## Требования

- **Node.js** (для `npx`) или **Docker** (для Docker-варианта)
- **Cursor** версии, поддерживающей MCP

## Дополнительные MCP серверы

Вы можете добавить несколько MCP серверов в один файл:

```json
{
  "mcpServers": {
    "sequential-thinking": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-sequential-thinking"]
    },
    "filesystem": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-filesystem"]
    }
  }
}
```

## Полезные ссылки

- [Официальная документация Cursor MCP](https://docs.cursor.com/context/model-context-protocol)
- [GitHub: Sequential Thinking MCP](https://github.com/modelcontextprotocol/servers/tree/HEAD/src/sequentialthinking)
- [MCP Registry](https://github.com/modelcontextprotocol/servers)

