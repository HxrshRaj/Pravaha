
from pravaha.auth.security import (
    create_access_token,
    decode_access_token,
    hash_api_key,
    hash_password,
    new_api_key,
    verify_password,
)
from pravaha.kafka.producer import partition_for_key


def test_password_hash_roundtrip():
    h = hash_password("s3cret-pw")
    assert h != "s3cret-pw"
    assert verify_password("s3cret-pw", h)
    assert not verify_password("wrong", h)


def test_api_key_hash_is_deterministic_and_opaque():
    k = new_api_key()
    assert k.startswith("pvh_")
    assert hash_api_key(k) == hash_api_key(k)
    assert hash_api_key(k) != k
    assert len(hash_api_key(k)) == 64


def test_jwt_roundtrip_and_tamper():
    tok = create_access_token("user-1", "ADMIN")
    payload = decode_access_token(tok)
    assert payload["sub"] == "user-1"
    assert payload["role"] == "ADMIN"
    assert decode_access_token(tok + "x") is None


def test_partitioning_is_deterministic_and_bounded():
    for key in ("order-123", "user-9", "corr-abc"):
        p1 = partition_for_key(key, 6)
        p2 = partition_for_key(key, 6)
        assert p1 == p2
        assert 0 <= p1 < 6
    # different keys spread across partitions
    seen = {partition_for_key(f"order-{i}", 6) for i in range(200)}
    assert len(seen) >= 4
