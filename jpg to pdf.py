from PIL import Image

def jpg_to_pdf(jpg_path, pdf_path):
    """
    Converts a single JPG image to a PDF document.

    Args:
        jpg_path (str): The path to the input JPG file.
        pdf_path (str): The path where the output PDF file will be saved.
    """
    try:
        image = Image.open(jpg_path)
        # Ensure the image is in RGB mode before saving as PDF
        if image.mode == "RGBA":
            image = image.convert("RGB")
        image.save(pdf_path, "PDF", resolution=100.0)
        print(f"Successfully converted '{jpg_path}' to '{pdf_path}'")
    except FileNotFoundError:
        print(f"Error: JPG file not found at '{jpg_path}'")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Example usage:
    input_jpg_file = r"P:\Documentation\MBA\MBA Completion Certificate.jpg"  # Replace with your JPG file name
    output_pdf_file = r"P:\Documentation\MBA\MBA Completion Certificate.pdf"  # Desired output PDF file name

    # Create a dummy JPG file for demonstration if it doesn't exist
    try:
        Image.new('RGB', (60, 30), color = 'red').save(input_jpg_file)
        print(f"Created a dummy '{input_jpg_file}' for demonstration.")
    except Exception as e:
        print(f"Could not create dummy JPG: {e}")

    jpg_to_pdf(input_jpg_file, output_pdf_file)

    # You can also use it with different files:
    # jpg_to_pdf("my_photo.jpg", "my_photo.pdf")