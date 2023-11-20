import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from keras.preprocessing.text import Tokenizer
from keras.preprocessing.sequence import pad_sequences
from keras.models import Sequential
from keras.layers import Embedding, Flatten, Dense

# Assuming df is your DataFrame with 'Activity' and 'Sector' columns
X = df['Activity']
y = df['Sector']

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Convert text to numerical data
tokenizer = Tokenizer()
tokenizer.fit_on_texts(X_train)
X_train_seq = tokenizer.texts_to_sequences(X_train)
X_test_seq = tokenizer.texts_to_sequences(X_test)

# Pad sequences to have consistent length
X_train_pad = pad_sequences(X_train_seq)
X_test_pad = pad_sequences(X_test_seq, maxlen=X_train_pad.shape[1])

# Encode labels
le = LabelEncoder()
y_train_encoded = le.fit_transform(y_train)
y_test_encoded = le.transform(y_test)

# Build a simple neural network model
model = Sequential()
model.add(Embedding(input_dim=len(tokenizer.word_index) + 1, output_dim=50, input_length=X_train_pad.shape[1]))
model.add(Flatten())
model.add(Dense(64, activation='relu'))
model.add(Dense(len(sector_keywords), activation='softmax'))

# Compile the model
model.compile(optimizer='adam', loss='sparse_categorical_crossentropy', metrics=['accuracy'])

# Train the model
model.fit(X_train_pad, y_train_encoded, epochs=5, batch_size=32, validation_split=0.2)

# Evaluate the model
test_loss, test_acc = model.evaluate(X_test_pad, y_test_encoded)
print(f'Test accuracy: {test_acc}')

# Make predictions for your CSV data
csv_data = ['Cultivo de arroz', 'Pesca de peixes em água salgada', ...]  # Replace with your actual data
csv_sequences = tokenizer.texts_to_sequences(csv_data)
csv_pad = pad_sequences(csv_sequences, maxlen=X_train_pad.shape[1])
csv_predictions = model.predict_classes(csv_pad)

# Decode predictions back to sector names
csv_predicted_sectors = le.inverse_transform(csv_predictions)
print(csv_predicted_sectors)
