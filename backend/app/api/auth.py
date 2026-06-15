from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
import os
from app.core.security import Token, verify_password, create_access_token, get_password_hash, ACCESS_TOKEN_EXPIRE_MINUTES
from datetime import timedelta, timezone, datetime

router = APIRouter()

# En mémoire, utilisateur admin par défaut pour la démo
ADMIN_PASS_HASH = get_password_hash(os.getenv("ADMIN_PASSWORD", "admin123"))
DEFAULT_USERS = {
    "admin": {
        "username": "admin",
        "hashed_password": ADMIN_PASS_HASH,
    }
}

@router.post("/token", response_model=Token, tags=["Authentification"])
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user_dict = DEFAULT_USERS.get(form_data.username)
    if not user_dict:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Nom d'utilisateur ou mot de passe incorrect",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not verify_password(form_data.password, user_dict["hashed_password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Nom d'utilisateur ou mot de passe incorrect",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user_dict["username"]}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}
