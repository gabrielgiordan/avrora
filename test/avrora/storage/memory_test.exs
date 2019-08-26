defmodule Avrora.Storage.MemoryTest do
  use ExUnit.Case, async: true
  doctest Avrora.Storage.Memory

  alias Avrora.Storage.Memory

  setup do
    pid = start_supervised!({Memory, name: :test_schema_storage})

    %{schema_storage: pid}
  end

  describe "put/3" do
    test "when key is new", %{schema_storage: pid} do
      assert get(pid, "my-key") == {:ok, nil}
      assert put(pid, "my-key", schema()) == {:ok, schema()}
      assert get(pid, "my-key") == {:ok, schema()}
    end

    test "when key already exists", %{schema_storage: pid} do
      {:ok, _} = put(pid, "my-key", schema())

      assert get(pid, "my-key") == {:ok, schema()}
      assert put(pid, "my-key", new_schema()) == {:ok, new_schema()}
      assert get(pid, "my-key") == {:ok, new_schema()}
    end
  end

  describe "get/2" do
    test "when key already exists", %{schema_storage: pid} do
      {:ok, _} = put(pid, "my-key", schema())

      assert get(pid, "my-key") == {:ok, schema()}
    end

    test "when key does not exist", %{schema_storage: pid} do
      assert get(pid, "my-key") == {:ok, nil}
    end
  end

  describe "expire/3" do
    test "when key already exists", %{schema_storage: pid} do
      {:ok, _} = put(pid, "my-key-to-expire", schema())
      {:ok, _} = expire(pid, "my-key-to-expire", 200)

      assert get(pid, "my-key-to-expire") == {:ok, schema()}
      Process.sleep(200)
      assert get(pid, "my-key-to-expire") == {:ok, nil}
    end

    test "when key does not exist", %{schema_storage: pid} do
      {:ok, _} = expire(pid, "my-key-to-expire", 100)

      assert get(pid, "my-key-to-expire") == {:ok, nil}
      Process.sleep(100)
      assert get(pid, "my-key-to-expire") == {:ok, nil}
    end
  end

  describe "delete/2" do
    test "when key already exists", %{schema_storage: pid} do
      {:ok, _} = put(pid, "my-key-to-delete", schema())

      assert get(pid, "my-key-to-delete") == {:ok, schema()}
      assert {:ok, true} = delete(pid, "my-key-to-delete")
      assert get(pid, "my-key-to-delete") == {:ok, nil}
    end

    test "when key does not exist", %{schema_storage: pid} do
      assert get(pid, "my-key-to-delete") == {:ok, nil}
      assert {:ok, true} = delete(pid, "my-key-to-delete")
      assert get(pid, "my-key-to-delete") == {:ok, nil}
    end
  end

  defp get(pid, key), do: Memory.get(pid, key)
  defp put(pid, key, value), do: Memory.put(pid, key, value)
  defp delete(pid, key), do: Memory.delete(pid, key)
  defp expire(pid, key, ttl), do: Memory.expire(pid, key, ttl)

  defp schema,
    do: %Avrora.Schema{
      id: 1,
      schema: [],
      raw_schema:
        ~s({"type": "record", "name": "one", "fields": [{"name": "id", "type": "integer"}]})
    }

  defp new_schema,
    do: %Avrora.Schema{
      id: 1,
      schema: [],
      raw_schema:
        ~s({"type": "record", "name": "two", "fields": [{"name": "greeting", "type": "string"}]})
    }
end
