from cryptography.fernet import Fernet

if __name__ == "__main__":
	
	try:
		# Key generation
		key = Fernet.generate_key()
		
		# Key saving
		with open("secret.txt", "wb") as file:
			file.write(key)
			
		with open("./kafka-consumer-azure/secret.txt", "wb") as file:
			file.write(key)
			
		with open("./kafka-consumer-firebase/secret.txt", "wb") as file:
			file.write(key)
		
		print("\nKey generated successfully!")
	
	except:
		print("\nError in key generation!")